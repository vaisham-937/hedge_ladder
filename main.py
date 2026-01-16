from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.middleware.sessions import SessionMiddleware
from passlib.context import CryptContext
from kiteconnect import KiteConnect
from pydantic import BaseModel
import asyncio
from typing import Dict, Optional, Any, List
from ladder_engine import LadderEngine, LadderSettings
import redis, json, time, datetime, pytz, os
import logging
from fastapi import WebSocket, WebSocketDisconnect
from collections import defaultdict
import time

# ----------------- APP   -------------------
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="SUPER_SECRET_KEY_123", same_site="lax")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# --- Named loggers ---
tick_log = logging.getLogger("TICKS")
ws_log   = logging.getLogger("WS")
lad_log  = logging.getLogger("LADDER")

# ------------------------------------------------
# Add an in-memory Tick Hub
# ------------------------------------------------

# 🔥 GLOBAL RAM SYMBOL ↔ TOKEN MAP
SYMBOL_TOKEN_RAM: Dict[int, Dict[str, int]] = defaultdict(dict)
TOKEN_SYMBOL_RAM: Dict[int, Dict[int, str]] = defaultdict(dict)


from collections import defaultdict
from fastapi import WebSocket, Query
import asyncio


# ============================================================
# ✅ RAM: token/symbol map (NO tick storage in redis)
# ============================================================

# per user: symbol->token (from ingest ticks OR fallback map)
SYMBOL_TOKEN_RAM: Dict[int, Dict[str, int]] = defaultdict(dict)
# per user: token->symbol
TOKEN_SYMBOL_RAM: Dict[int, Dict[int, str]] = defaultdict(dict)

# ===== Instruments file (NO REDIS mapping) =====
INSTR_FILE = os.path.join(BASE_DIR, "nse_eq_instruments.json")

# ✅ OPTIONAL: Global symbol->token map from a local json file (recommended)
# Create it using fetch_nse_instruments.py => nse_eq_instruments.json
GLOBAL_SYMBOL_TOKEN: Dict[str, int] = {}
GLOBAL_TOKEN_SYMBOL: Dict[int, str] = {}

def load_instruments_file():
    global GLOBAL_SYMBOL_TOKEN, GLOBAL_TOKEN_SYMBOL
    path = os.path.join(BASE_DIR, "nse_eq_instruments.json")
    if not os.path.exists(path):
        ws_log.warning("⚠️ nse_eq_instruments.json not found. Ladder token may be missing until first tick arrives.")
        return
    try:
        data = json.load(open(path, "r", encoding="utf-8"))
        GLOBAL_SYMBOL_TOKEN = {k.upper(): int(v) for k, v in (data.get("symbol_to_token") or {}).items()}
        GLOBAL_TOKEN_SYMBOL = {int(k): v.upper() for k, v in (data.get("token_to_symbol") or {}).items()}
        ws_log.info("✅ Instruments loaded: %d symbols", len(GLOBAL_SYMBOL_TOKEN))
    except Exception as e:
        ws_log.exception("❌ Failed to load instruments json: %s", e)

load_instruments_file()




# ============================================================
# ✅ WS Tokens Control (if your kite_ws_worker reads tokens list)
# NOTE: if your worker still reads redis ws:{user_id}:tokens, keep that logic too.
# ============================================================

WS_TOKENS_RAM: Dict[int, set] = defaultdict(set)

def add_ws_token(user_id: int, token: int):
    if token and token > 0:
        WS_TOKENS_RAM[user_id].add(int(token))


@app.get("/api/ws-tokens")
def api_ws_tokens(request: Request, user_id: int = Query(None)):
    # If called from browser session
    sess_uid = request.session.get("user_id")

    # If called by worker (local)
    if user_id is not None:
        # allow only localhost calls (basic guard)
        client = request.client.host if request.client else ""
        if client not in ("127.0.0.1", "localhost"):
            return JSONResponse({"error": "forbidden"}, 403)
        uid = int(user_id)
    else:
        if not sess_uid:
            return JSONResponse({"error": "Not logged in"}, 401)
        uid = int(sess_uid)

    toks = sorted(list(WS_TOKENS_RAM.get(uid, set())))
    return {"user_id": uid, "tokens": toks}


class TickHub:
    def __init__(self):
        self.ticks = defaultdict(dict)     # user → token → tick
        self.clients = defaultdict(set)   # user → websocket set

    def update_tick(self, user_id, token, tick):
        self.ticks[user_id][token] = tick
        logging.getLogger("TICKS").debug("📥 RAM tick user=%s token=%s", user_id, token)

    async def register(self, user_id, ws):
        await ws.accept()
        self.clients[user_id].add(ws)

    def unregister(self, user_id, ws):
        self.clients[user_id].discard(ws)

    async def broadcast(self, user_id, payload):
        dead = []
        for ws in self.clients.get(user_id, []):
            try:
                await ws.send_json(payload)
            except:
                dead.append(ws)

        for ws in dead:
            self.unregister(user_id, ws)
    
    def get_snapshot_for_user(self, user_id: int, only_symbols: Optional[set] = None) -> Dict[str, Any]:
        """
        Returns latest ticks snapshot (symbol keyed) for dashboard on WS connect.
        """
        out = {}
        user_ticks = self.ticks.get(user_id, {})  # token -> tick dict

        for tok, t in user_ticks.items():
            sym = (t.get("symbol") or "").upper()
            if not sym:
                continue
            if only_symbols and sym not in only_symbols:
                continue

            ltp  = float(t.get("ltp", 0) or 0)
            prev = float(t.get("prev", ltp) or ltp)
            high = float(t.get("high", ltp) or ltp)
            low  = float(t.get("low", ltp) or ltp)
            opn  = float(t.get("open", prev) or prev)

            out[sym] = {
                "symbol": sym,
                "ltp": ltp,
                "tbq": int(t.get("tbq", 0) or 0),
                "tsq": int(t.get("tsq", 0) or 0),
                "prev": prev,
                "open": opn,
                "high": high,
                "low": low,
            }
        return out

TICK_HUB = TickHub()

async def circuit_prefetch_loop():
    """
    Background loop: fetch circuit for active symbols in WS tokens set.
    Uses get_kite_for_user(user_id) so per-user auth works.
    """
    while True:
        try:
            # every 60 sec check
            await asyncio.sleep(60)

            if not _is_market_time_for_circuit():
                continue

            # for each user, find symbols we are tracking (from RAM token map)
            for uid, tokmap in TOKEN_SYMBOL_RAM.items():
                if not tokmap:
                    continue

                # pick unique symbols from token->symbol map
                syms = set([s for s in tokmap.values() if s])

                if not syms:
                    continue

                # fetch circuits in small batches (avoid heavy calls)
                kite = None
                try:
                    kite = get_kite_for_user(int(uid))
                except:
                    continue
                # Kite quote supports multiple instruments  We fetch only those missing/expired
                to_fetch = []
                for sym in list(syms)[:200]:
                    c = get_cached_circuit(sym)
                    if not c or (time.time() - float(c.get("ts", 0) or 0)) > CIRCUIT_TTL_SEC:
                        to_fetch.append(f"NSE:{sym}")

                if not to_fetch:
                    continue

                # chunk to avoid huge payload
                CHUNK = 50
                for i in range(0, len(to_fetch), CHUNK):
                    batch = to_fetch[i:i+CHUNK]
                    try:
                        q = kite.quote(batch)  # dict keyed by "NSE:SYMBOL"
                        for k, v in (q or {}).items():
                            sym = str(k).split(":")[-1].upper()
                            upper = v.get("upper_circuit_limit")
                            lower = v.get("lower_circuit_limit")
                            payload = {
                                "upper": float(upper) if upper is not None else None,
                                "lower": float(lower) if lower is not None else None,
                                "ts": time.time()
                            }
                            CIRCUIT_RAM[sym] = payload
                            r.setex(f"circuit:{sym}", CIRCUIT_TTL_SEC, json.dumps(payload))
                    except:
                        continue

        except Exception:
            continue

@app.on_event("startup")
async def _startup_tasks():
    asyncio.create_task(circuit_prefetch_loop(), name="circuit_prefetch_loop")


# ------------------------------------------------- 
# WebSocket route for dashboard ticks (NO Redis) 
# -------------------------------------------------
@app.websocket("/ws/ticks")
async def ws_ticks(ws: WebSocket):
    uid = ws.query_params.get("user_id")
    if not uid or not uid.isdigit():
        await ws.close(code=1008)
        return

    uid = int(uid)
    await TICK_HUB.register(uid, ws)
    print("🟢 Dashboard WS connected:", uid)

    # ✅ NEW: Push snapshot immediately so UI shows LTP without waiting
    try:
        snap = TICK_HUB.get_snapshot_for_user(uid)  # symbol -> payload

        # ✅ add circuit values into snapshot
        ticks = []
        for t in snap.values():
            sym = (t.get("symbol") or "").upper()
            if sym:
                c = get_cached_circuit(sym)
                t["upper_circuit"] = c.get("upper")
                t["lower_circuit"] = c.get("lower")
            ticks.append(t)

        await ws.send_json({"type": "snapshot", "ticks": ticks})

    except Exception:
        pass

    try:
        while True:
            await asyncio.sleep(30)   # keep alive
    except:
        pass
    finally:
        TICK_HUB.unregister(uid, ws)
        print("🔴 Dashboard WS closed:", uid)


@app.websocket("/ws/ingest")
async def ws_ingest(ws: WebSocket):
    uid = ws.query_params.get("user_id")
    if not uid or not uid.isdigit():
        await ws.close(code=1008)
        return
    uid = int(uid)
    await ws.accept()
    ws_log.info("📥 INGEST connected user=%s", uid)
    try:
        while True:
            # ✅ receive TEXT (worker sends json.dumps)
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("type") != "ticks":
                continue
            ticks = msg.get("ticks") or []
            if not ticks:
                continue
            ws_log.info("📦 Received %d ticks from Zerodha user=%s", len(ticks), uid)
            eng = ENGINE_HUB.get(uid)
            for t in ticks:
    # -------- normalize token/ltp ----------
                token = t.get("token") or t.get("instrument_token") or t.get("instrumentToken")
                ltp   = t.get("ltp")   or t.get("last_price")       or t.get("lastPrice")

                try:
                    token = int(token or 0)
                    ltp   = float(ltp or 0)
                except:
                    continue

                if token <= 0 or ltp <= 0:
                    continue

                # -------- normalize symbol ----------
                sym = (t.get("symbol") or t.get("tradingsymbol") or t.get("tradingSymbol") or "").upper().strip()
                if not sym:
                    sym = TOKEN_SYMBOL_RAM[uid].get(token, "")
                if not sym:
                    sym = GLOBAL_TOKEN_SYMBOL.get(token, "")
                if sym:
                    TOKEN_SYMBOL_RAM[uid][token] = sym

                # -------- normalize quantities ----------
                tbq = t.get("tbq") or t.get("buy_quantity") or t.get("buyQuantity") or 0
                tsq = t.get("tsq") or t.get("sell_quantity") or t.get("sellQuantity") or 0
                try:
                    tbq = int(tbq or 0)
                    tsq = int(tsq or 0)
                except:
                    tbq, tsq = 0, 0

                # -------- normalize ohlc ----------
                ohlc = t.get("ohlc") or {}
                op   = t.get("open") or ohlc.get("open")
                hi   = t.get("high") or ohlc.get("high")
                lo   = t.get("low")  or ohlc.get("low")
                prev = t.get("prev") or ohlc.get("close")  # prev-close

                op   = float(op or prev or ltp)
                hi   = float(hi or ltp)
                lo   = float(lo or ltp)
                prev = float(prev or ltp)

                # ---------- TOKEN MAP (RAM) ----------
                if sym:
                    SYMBOL_TOKEN_RAM[uid][sym] = token
                    add_ws_token(uid, token)
                    if eng:
                        if getattr(eng, "SYMBOL_TOKEN_RAM", None) is None:
                            eng.SYMBOL_TOKEN_RAM = {}
                        eng.SYMBOL_TOKEN_RAM.setdefault(uid, {})[sym] = token

                # ---------- TickHub RAM ----------
                TICK_HUB.update_tick(uid, token, {
                    "ltp": ltp, "tbq": tbq, "tsq": tsq,
                    "prev": prev, "open": op, "high": hi, "low": lo,
                    "symbol": sym, "token": token
                })

                # ---------- LADDER ----------
                if eng:
                    try:
                        await eng.ingest_tick(token, ltp)
                    except Exception:
                        lad_log.exception("Ladder ingest failed")

                # ---------- DASHBOARD WS ----------
                if sym:
                    c = get_cached_circuit(sym)
                    upper = c.get("upper")
                    lower = c.get("lower")
                    await TICK_HUB.broadcast(uid, {
                        "symbol": sym,
                        "ltp": ltp,
                        "tbq": tbq,
                        "tsq": tsq,
                        "prev": prev,
                        "open": op,     
                        "high": hi,
                        "low": lo,
                        "upper_circuit": upper,  
                        "lower_circuit": lower,
                    })
    except Exception:
        ws_log.exception("❌ INGEST WS crashed user=%s", uid)

    finally:
        ws_log.warning("📤 INGEST disconnected user=%s", uid)



# ------------------------------------------------
# Ladder Engine Hub (per-user)
# ------------------------------------------------
ENGINE_HUB: Dict[int, LadderEngine] = {}

def get_kite_for_user(user_id: int) -> KiteConnect:
    """
    ✅ api_key: from saved credentials hash (zerodha:keys:{user_id})
    ✅ access_token: ONLY from redis key access_token:{user_id} (24h TTL)
    """
    keys = r.hgetall(f"zerodha:keys:{user_id}")
    api_key = (keys or {}).get("api_key")
    access_token = r.get(f"access_token:{user_id}")

    if not api_key:
        raise RuntimeError("Zerodha API key missing. Save credentials first.")
    if not access_token:
        raise RuntimeError("Zerodha not connected / token expired")

    kite = KiteConnect(api_key=str(api_key))
    kite.set_access_token(str(access_token))
    return kite

async def get_engine_for_user(user_id: int) -> LadderEngine:
    keys = r.hgetall(f"zerodha:keys:{user_id}")
    api_key = (keys or {}).get("api_key")
    access_token = r.get(f"access_token:{user_id}")

    if not api_key:
        raise RuntimeError("Zerodha API key missing. Save credentials first.")
    if not access_token:
        raise RuntimeError("Zerodha not connected / token expired")

    redis_url = REDIS_URL or "redis://127.0.0.1:6379/0"

    eng = ENGINE_HUB.get(int(user_id))
    if eng is None or eng.api_key != str(api_key) or eng.access_token != str(access_token):
        eng = LadderEngine(
            redis_url=redis_url,
            user_id=int(user_id),
            api_key=str(api_key),
            access_token=str(access_token),
        )
        ENGINE_HUB[int(user_id)] = eng
        await eng.start()
    else:
        await eng.start()

    # inject RAM mapping
    eng.SYMBOL_TOKEN_RAM = SYMBOL_TOKEN_RAM
    return eng




# ------------------------------------------------
# REDIS
REDIS_URL = os.getenv("REDIS_URL")
if REDIS_URL:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
else:
    r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

# ============================================================
# ✅ CIRCUIT CACHE (Redis + in-memory TTL)
# ============================================================
CIRCUIT_RAM: Dict[str, Dict[str, Any]] = {}   # symbol -> {"upper":..,"lower":..,"ts":..}
CIRCUIT_TTL_SEC = 30 * 60                     # 30 min (same day ok)

def _now_ist():
    ist = pytz.timezone("Asia/Kolkata")
    return datetime.datetime.now(ist)

def _is_market_time_for_circuit():
    # circuit values once/day, but we keep it simple
    now = _now_ist()
    if now.weekday() >= 5:
        return False
    return True

def get_cached_circuit(symbol: str) -> Dict[str, Any]:
    """
    Fast path: RAM -> Redis.
    Returns dict: {"upper": float|None, "lower": float|None, "ts": epoch}
    """
    sym = symbol.upper().strip()
    if not sym:
        return {}

    # 1) RAM cache
    c = CIRCUIT_RAM.get(sym)
    if c and (time.time() - float(c.get("ts", 0) or 0)) < CIRCUIT_TTL_SEC:
        return c

    # 2) Redis cache
    raw = r.get(f"circuit:{sym}")
    if raw:
        try:
            d = json.loads(raw)
            # keep in RAM too
            CIRCUIT_RAM[sym] = d
            return d
        except:
            pass

    return {}



# ------------------------------------------------
# PASSWORD HASH
pwd = CryptContext(schemes=["bcrypt"])

# ------------------------------------------------
# MODELS
class AuthForm(BaseModel):
    username: str
    password: str

class CredentialForm(BaseModel):
    api_key: str
    api_secret: str

# ------------------------------------------------
# UI ROUTES
# ------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def index():
    return open(os.path.join(BASE_DIR, "templates/index.html"), encoding="utf-8").read()

# ------------------------------------------------
# AUTHENTICATION 
# ------------------------------------------------
@app.post("/register")
def register(username: str = Form(...), password: str = Form(...)):
    # username already exists?
    if r.exists(f"username_to_id:{username}"):
        return RedirectResponse("/?error=exists", status_code=302)

    # 🔢 generate numeric user_id
    user_id = r.incr("user_counter")   # 1,2,3...

    # map username → id
    r.set(f"username_to_id:{username}", user_id)

    # store user data  create user hash safely
    r.hset(f"user:{user_id}", "username", str(username))
    r.hset(f"user:{user_id}", "password", pwd.hash(password[:72]))

    return RedirectResponse("/", status_code=302)


@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    user_id = r.get(f"username_to_id:{username}")
    if not user_id:
        return RedirectResponse("/?error=login", status_code=302)

    user = r.hgetall(f"user:{user_id}")
    if not user:
        return RedirectResponse("/?error=login", status_code=302)

    # ✅ prevent bcrypt crash if user types/pastes very long input
    try:
        ok = pwd.verify(password[:72], user["password"])
    except Exception as e:
        print("❌ Password verify failed:", e)
        return RedirectResponse("/?error=hash", status_code=302)
    if not ok:
        return RedirectResponse("/?error=login", status_code=302)

    request.session["user_id"] = int(user_id)
    request.session["username"] = username
    return RedirectResponse("/dashboard", status_code=303)



@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/")


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    if "user_id" not in request.session:
        return RedirectResponse("/")

    user_id = request.session["user_id"]          # int
    username = request.session.get("username","")

    html = open(
        os.path.join(BASE_DIR, "templates", "dashboard.html"),
        encoding="utf-8"
    ).read()

    html = html.replace("{{USERNAME}}", str(username))
    html = html.replace("{{USER_ID}}", str(user_id))

    return html

# ------------------------------------------------
# SAVE ZERODHA CREDENTIALS (Dashboard tab)
# ------------------------------------------------
@app.post("/api/save-credentials")
async def save_credentials(request: Request):
    data = await request.json()
    user = request.session["user_id"]

    api_key = data.get("api_key")
    api_secret = data.get("api_secret")

    if not api_key or not api_secret:
        return JSONResponse({"error": "API key & secret required"}, 400)

    r.hset(f"zerodha:keys:{user}", "api_key", str(api_key))
    r.hset(f"zerodha:keys:{user}", "api_secret", str(api_secret))

    return {"status": "saved"}

# ------------------------------------------------
# ZERODHA LOGIN (Kite)
# ------------------------------------------------
@app.get("/connect/zerodha")
def connect_zerodha(request: Request):
    user = request.session.get("user_id")
    if not user:
        return RedirectResponse("/")

    keys = r.hgetall(f"zerodha:keys:{user}")
    if not keys:
        return RedirectResponse("/dashboard?error=nokeys")

    kite = KiteConnect(api_key=keys["api_key"])
    print("🔗🔗 Redirecting to Zerodha 🔗🔗")
    return RedirectResponse(kite.login_url())

# -------------------------------------------------
# ZERODHA CALLBACK
# -------------------------------------------------
@app.get("/zerodha/callback")
def zerodha_callback(
    request: Request,
    request_token: str = None,
    status: str = None
):
    # 1️⃣ Basic validation
    if not request_token or status != "success":
        return RedirectResponse("/dashboard?error=zerodha_failed")

    # 2️⃣ Get logged-in user
    user_id = request.session.get("user_id")
    if not user_id:
        return RedirectResponse("/")

    # 3️⃣ Fetch Zerodha keys (already stored earlier)
    keys = r.hgetall(f"zerodha:keys:{user_id}")
    if not keys or "api_key" not in keys or "api_secret" not in keys:
        return RedirectResponse("/dashboard?error=missing_keys")

    # 4️⃣ Generate access token
    kite = KiteConnect(api_key=keys["api_key"])
    try:
        session = kite.generate_session(
            request_token=request_token,
            api_secret=keys["api_secret"]
        )
    except Exception as e:
        print("❌ Zerodha session error:", e)
        return RedirectResponse("/dashboard?error=token")

    access_token = session["access_token"]

    # 5️⃣ SAVE TO REDIS (🔥 THIS IS THE FIX 🔥)
    TTL = 60 * 60 * 6   # ~6 hours (same trading day)

    r.setex(f"access_token:{user_id}", TTL, access_token)
    r.setex(f"api_key:{user_id}", TTL, keys["api_key"])

    # 6️⃣ DEBUG CONFIRMATION (VERY IMPORTANT)
    print("✅ access_token stored 24h user:", user_id, "ttl:", r.ttl(f"access_token:{user_id}"))
    print("✅ Zerodha connected for user:", user_id)
    return RedirectResponse("/dashboard")



@app.get("/api/zerodha-status")
def zerodha_status(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return {"connected": False}
    
    keys = r.hgetall(f"zerodha:keys:{user_id}")
    api_key = (keys or {}).get("api_key")
    access_token = r.get(f"access_token:{user_id}")

    if api_key and access_token:
        ttl = r.ttl(f"access_token:{user_id}")
        if ttl <= 0:
            return {"connected": False}
        return {
            "connected": True,
            "expiry": int(time.time()) + ttl
}
    return {"connected": False}

# ------------------------------------------------
# MARKET STATUS (09:15 - 15:30)
# ------------------------------------------------
@app.get("/api/market-status")
def market_status():
    """
    Returns whether NSE market is open (simple time-window guard).
    NSE regular hours: 09:15 - 15:30 IST (mon-fri). Weekend => closed.
    """
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.datetime.now(ist)

    # Weekend
    if now.weekday() >= 5:
        return {"market_open": False,
                "reason": "Weekend",
                "now": int(now.timestamp()),
                "open_at": None,
                "close_at": None}

    # Market timings
    open_dt = now.replace(hour=9, minute=15, second=0)
    close_dt = now.replace(hour=15, minute=30, second=0)

    # If before open, market closed (pre-open)
    if now < open_dt:
        return {
            "market_open": False,
            "reason": "Pre-open",
            "now": int(now.timestamp()),
            "open_at": int(open_dt.timestamp()),
            "close_at": int(close_dt.timestamp())
        }

    # After close
    if now >= close_dt:
        # show next open (tomorrow)
        tomorrow = now + datetime.timedelta(days=1)
        next_open = tomorrow.replace(hour=9, minute=15, second=0, microsecond=0)
        # if tomorrow weekend, compute next weekday
        while next_open.weekday() >= 5:
            next_open = next_open + datetime.timedelta(days=1)
            next_open = next_open.replace(hour=9, minute=15, second=0, microsecond=0)
        return {
            "market_open": False,
            "reason": "Closed (after market close)",
            "now": int(now.timestamp()),
            "open_at": int(next_open.timestamp()),
            "close_at": int(close_dt.timestamp())
        }
    # Market is open
    return {
        "market_open": True,
        "reason": "Open",
        "now": int(now.timestamp()),
        "open_at": int(open_dt.timestamp()),
        "close_at": int(close_dt.timestamp())
    }


# ------------------------------------------------
# API TICKS 
# ------------------------------------------------
# @app.get("/api/ticks")
# def get_ticks(request: Request, symbols: str):
#     user_id = request.session["user_id"]  # Add this
#     result = {}
#     for s in symbols.split(","):
#         token = r.get(f"symbol_token:{s}")
#         upper = r.get(f"circuit:{s}")
#         if not token:
#             continue
#         d = r.hgetall(f"tick:{user_id}:{token}")  # Fix key to include user_id
#         if d:
#             ltp = float(d.get("ltp", 0))
#             tbq = int(d.get("tbq", 0))
#             tsq = int(d.get("tsq", 0))
#             openp = float(d.get("prev", ltp))
#             high = float(d.get("high", ltp))
#             low  = float(d.get("low", ltp))
#             pct_from_open = round(((ltp - openp) / openp) * 100, 2) if openp else 0
#             pct_from_high = round(((ltp - high) / high) * 100, 2) if high else 0
#             pct_from_low  = round(((ltp - low) / low) * 100, 2) if low else 0
#             result[s] = {
#                 "ltp": ltp,
#                 "tbq": tbq,
#                 "tsq": tsq,
#                 "bto": round(ltp * tbq),
#                 "sto": round(ltp * tsq),
#                 "pct": round(((ltp - openp) / openp) * 100, 2) if openp else 0,
#                 "from_open": pct_from_open,
#                 "from_high": pct_from_high,
#                 "from_low": pct_from_low,
#                 "high": high,
#                 "low": low,
#             }
#     return result




@app.get("/api/circuit/{symbol}")
async def get_circuit(symbol: str):
    raw = r.get(f"circuit:{symbol.upper()}")
    return json.loads(raw) if raw else {}


# ------------------------------------------------
# CHARTINK WEBHOOK 
# ------------------------------------------------
@app.post("/webhook/chartink/{user_id}")
async def chartink_webhook(request: Request, user_id: int):
    data = await request.json()

    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.datetime.now(ist)
    if now.weekday() >= 5:
        return {"status": "ignored", "reason": "Weekend"}

    scan_name = data.get("scan_name", "Chartink")
    stocks_str = data.get("stocks", "")

    today = now.strftime("%Y-%m-%d")
    redis_key = f"chartink_alerts:{user_id}:{today}"
    seen_key = f"chartink_seen:{user_id}:{today}:{scan_name}"

    new_stocks = []

    for s in stocks_str.split(","):
        symbol = s.strip()
        if not symbol:
            continue

        if r.sismember(seen_key, symbol):
            continue

        # ✅ mark seen
        r.sadd(seen_key, symbol)
        new_stocks.append(symbol)

        # 🔥 TOKEN ENSURE
        tok = SYMBOL_TOKEN_RAM.get(user_id, {}).get(symbol) or GLOBAL_SYMBOL_TOKEN.get(symbol)
        if tok:
            add_ws_token(user_id, int(tok))

    if not new_stocks:
        return {"status": "ignored"}

    packet = {
        "id": int(time.time() * 1000),
        "scan": scan_name,
        "stocks": new_stocks,
        "time": now.strftime("%H:%M:%S")
    }
    print(f"📥 Chartink Packet → {packet}")

    r.lpush(redis_key, json.dumps(packet))
    r.ltrim(redis_key, 0, 200)

    return {"status": "success"}

# ------------------------------------------------
# GET ALERTS (Dashboard)
# ------------------------------------------------
@app.get("/api/alerts")
def get_alerts(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return []

    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.datetime.now(ist).strftime("%Y-%m-%d")

    redis_key = f"chartink_alerts:{user_id}:{today}"
    raw = r.lrange(redis_key, 0, -1)

    return [json.loads(x) for x in raw]


# ------------------------------------------------
# LADDER ENGINE API
# ------------------------------------------------
class LadderStartReq(BaseModel):
    symbol: str
    side: str  # BUY or SELL
    settings_mode: str = "UNIVERSAL"  # UNIVERSAL or STOCK
    settings: Optional[Dict[str, Any]] = None  # optional override + save

@app.post("/api/ladder/start")
async def api_ladder_start(request: Request, body: LadderStartReq):
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)

    side = (body.side or "BUY").strip().upper()
    if side not in ("BUY", "SELL"):
        return JSONResponse({"error": "side must be BUY/SELL"}, 400)

    mode = (body.settings_mode or "UNIVERSAL").strip().upper()
    if mode not in ("UNIVERSAL", "STOCK"):
        return JSONResponse({"error": "settings_mode must be UNIVERSAL/STOCK"}, 400)

    symbol = body.symbol.strip().upper()

    try:
        # ✅ token resolve (RAM first, then global instruments)
        token = SYMBOL_TOKEN_RAM.get(int(user_id), {}).get(symbol) or GLOBAL_SYMBOL_TOKEN.get(symbol)

        if token:
            add_ws_token(int(user_id), int(token))

        eng = await get_engine_for_user(int(user_id))

        resp = await eng.start_ladder(
            symbol=symbol,
            started_side=side,
            settings_mode=mode,
            settings_payload=body.settings,
        )
        # if engine is waiting for feed, return clean response
        if isinstance(resp, dict) and resp.get("status") in ("waiting_for_feed", "waiting_for_ltp"):
            return {
                "status": resp["status"],
                "symbol": symbol,
                "message": resp.get("message", "Waiting for feed..."),
                "session": resp.get("session"),
            }

        return resp
    except Exception as e:
        logging.exception("Ladder start failed")
        return JSONResponse({"error": str(e)}, 500)


# =====================================================
# 🧱 LADDER bridge route — for dashboard squareoff button
# =====================================================
class LadderSquareoffReq(BaseModel):
    symbol: str

@app.post("/api/ladder/squareoff")
async def api_ladder_squareoff(request: Request, body: LadderSquareoffReq):
    # Reuse your existing single-squareoff logic
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    symbol = body.symbol.strip().upper()
    if not symbol:
        return JSONResponse({"error": "Symbol missing"}, 400)

    # Call your /api/squareoff code path internally
    kite = get_kite_for_user(int(user_id))
    pos = kite.positions().get("net", [])
    net_qty = 0
    for p in pos:
        if str(p.get("tradingsymbol", "")).upper() == symbol:
            net_qty = int(p.get("quantity", 0) or 0)
            break
    if net_qty == 0:
        # 🧹 Remove stale ladder entry immediately
        r.delete(f"ladder:state:{user_id}:{symbol}")
        return {"status": "no_position", "symbol": symbol}
    side = kite.TRANSACTION_TYPE_SELL if net_qty > 0 else kite.TRANSACTION_TYPE_BUY
    qty = abs(net_qty)
    oid = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=kite.EXCHANGE_NSE,
        tradingsymbol=symbol,
        transaction_type=side,
        quantity=qty,
        product=kite.PRODUCT_MIS,
        order_type=kite.ORDER_TYPE_MARKET,
    )
     #🧱 Clean up ladder state after placing exit
    r.delete(f"ladder:state:{user_id}:{symbol}")
    return {"status": "squareoff_placed", "symbol": symbol, "qty": qty, "order_id": oid}

@app.get("/api/ladder/sessions")
async def ladder_sessions(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return []
    eng = ENGINE_HUB.get(int(user_id))
    if not eng:
        return []
    return await eng.list_sessions()


@app.get("/api/ladder/state")
async def api_ladder_state(request: Request):
    """
    Returns ladder positions.
    Priority:
      1️⃣ In-memory ENGINE_HUB sessions (live state)
      2️⃣ Redis snapshots (persisted state)
    """
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse([], status_code=401)

    try:
        eng = ENGINE_HUB.get(int(user_id))
        out: List[Dict[str, Any]] = []

        # 1️⃣ Live sessions in memory
        if eng:
            out = await eng.list_sessions()

        # 2️⃣ Fallback to Redis (if no live sessions)
        if not out:
            keys = r.keys(f"ladder:state:{user_id}:*")
            for k in keys:
                raw = r.get(k)
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                    out.append(data)
                except Exception:
                    continue

        return JSONResponse(out)
    except Exception as e:
        print("❌ /api/ladder/state error:", e)
        return JSONResponse([], status_code=500)


# class KillReq(BaseModel):
#     enabled: bool = True

# @app.post("/api/kill-switch")
# async def api_kill_switch(request: Request):
#     user_id = request.session.get("user_id")
#     if not user_id:
#         return JSONResponse({"error": "Not logged in"}, 401)

#     data = await request.json()
#     enabled = bool(data.get("enabled", True))

#     try:
#         eng = await get_engine_for_user(int(user_id))
#         if enabled:
#             return await eng.kill_all()
#         else:
#             await eng.set_kill(False)
#             return {"status": "kill_cleared"}
#     except Exception as e:
#         return JSONResponse({"error": str(e)}, 500)

class KillSwitchReq(BaseModel):
    enabled: bool = True

@app.post("/api/kill-switch")
async def api_kill_switch(request: Request, body: KillSwitchReq):
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, status_code=401)

    try:
        eng = await get_engine_for_user(int(user_id))

        if body.enabled:
            await eng.kill_all()
        else:
            await eng.set_kill(False)

        return {"status": "ok", "enabled": body.enabled}

    except Exception as e:
        print("❌ Kill-switch failed:", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/squareoff") # Single exit
async def api_squareoff(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    data = await request.json()
    symbol = (data.get("symbol") or "").strip().upper()
    if not symbol:
        return JSONResponse({"error": "Symbol missing"}, 400)
    try:
        kite = get_kite_for_user(int(user_id))
        pos = kite.positions().get("net", [])
        net_qty = 0
        for p in pos:
            if str(p.get("tradingsymbol", "")).upper() == symbol:
                net_qty = int(p.get("quantity", 0) or 0)
                break
        if net_qty == 0:
            return {"status": "no_position", "symbol": symbol}
        side = kite.TRANSACTION_TYPE_SELL if net_qty > 0 else kite.TRANSACTION_TYPE_BUY
        qty = abs(net_qty)
        oid = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=side,
            quantity=qty,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
        )
        return {"status": "squareoff_placed", "symbol": symbol, "qty": qty, "order_id": oid}
    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)

@app.post("/api/squareoff_all")  # Exit all
async def api_squareoff_all(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)

    user_id = int(user_id)

    try:
        kite = get_kite_for_user(user_id)
        pos = kite.positions().get("net", [])

        placed = []
        symbols_to_cleanup = set()

        # 1) Place squareoff orders for all open positions
        for p in pos:
            sym = str(p.get("tradingsymbol", "")).upper()
            q = int(p.get("quantity", 0) or 0)
            if not sym or q == 0:
                continue

            side = kite.TRANSACTION_TYPE_SELL if q > 0 else kite.TRANSACTION_TYPE_BUY
            oid = kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=sym,
                transaction_type=side,
                quantity=abs(q),
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
            )

            placed.append({"symbol": sym, "qty": abs(q), "order_id": oid})
            symbols_to_cleanup.add(sym)

        # 2) Clean ladder state from MEMORY engine (very important)
        eng = ENGINE_HUB.get(user_id)
        if eng:
            for sym in list(symbols_to_cleanup):
                try:
                    # remove running session if exists
                    if hasattr(eng, "sessions") and sym in eng.sessions:
                        eng.sessions.pop(sym, None)
                except Exception:
                    pass

        # 3) Clean ladder state from REDIS so dashboard won't show stale rows
        # delete: state + locks (+ optionally stock settings)
        del_keys = []
        for sym in symbols_to_cleanup:
            del_keys.extend([
                f"ladder:state:{user_id}:{sym}",
                f"ladder:lock:{user_id}:{sym}:entry",
                f"ladder:lock:{user_id}:{sym}:add",
                f"ladder:lock:{user_id}:{sym}:exit",
                # Optional: comment next line if you want to keep saved stock-wise settings
                # f"ladder:settings:stock:{user_id}:{sym}",
            ])
        if del_keys:
            r.delete(*del_keys)

        # 4) Optional: remove tokens from ws set (if you want WS to stop tracking these)
        # If you want this, only remove tokens for symbols cleanup.
        # (depends on how you manage symbol->token mapping)
        for sym in symbols_to_cleanup:
            token = r.get(f"symbol_token:{sym}")
            if token and str(token).isdigit():
                r.srem(f"ws:{user_id}:tokens", int(token))

        return {"status": "squareoff_all_placed", "count": len(placed), "orders": placed}

    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)



@app.post("/api/trade")
async def place_trade(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    data = await request.json()
    symbol = str(data.get("symbol", "")).strip().upper()
    side = str(data.get("side", "BUY")).strip().upper()
    qty = int(data.get("qty", 0) or 0)
    if not symbol or qty <= 0 or side not in ("BUY", "SELL"):
        return JSONResponse({"error": "Invalid symbol/side/qty"}, 400)
    try:
        kite = get_kite_for_user(int(user_id))
        oid = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=(kite.TRANSACTION_TYPE_BUY if side == "BUY" else kite.TRANSACTION_TYPE_SELL),
            quantity=qty,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
        )
        return {"status": "order_placed", "symbol": symbol, "side": side, "qty": qty, "order_id": oid}
    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)

from fastapi import Query

@app.post("/api/ladder/delete")
async def delete_ladder(request: Request, symbol: str = Query(...)):
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)

    symbol = symbol.strip().upper()

    # 1) delete ladder state + locks + stock settings (SYNC redis calls)
    r.delete(
        f"ladder:state:{user_id}:{symbol}",
        f"ladder:lock:{user_id}:{symbol}:entry",
        f"ladder:lock:{user_id}:{symbol}:add",
        f"ladder:lock:{user_id}:{symbol}:exit",
        f"ladder:settings:stock:{user_id}:{symbol}",
    )

    # 2) remove token from ws set (control set)
    token = r.get(f"symbol_token:{symbol}")
    if token and str(token).isdigit():
        r.srem(f"ws:{user_id}:tokens", int(token))

    # 3) also remove in-memory ladder session (IMPORTANT)
    eng = ENGINE_HUB.get(int(user_id))
    if eng:
        try:
            # we'll implement this method in ladder_engine.py next:
            await eng.remove_session(symbol)
        except Exception:
            pass

    return {"status": "deleted", "symbol": symbol}

