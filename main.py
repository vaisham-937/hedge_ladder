from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.middleware.sessions import SessionMiddleware
from passlib.context import CryptContext
from kiteconnect import KiteConnect
from pydantic import BaseModel
import asyncio
from typing import Dict, Optional, Any, List
from ladder_engine import LadderEngine, LadderSettings
import redis.asyncio as aioredis
import json, time, datetime, pytz, os, re
import sys

import logging
from fastapi import WebSocket, WebSocketDisconnect
from collections import defaultdict
import time
# Removed invalid import


# ----------------- APP   -------------------
app = FastAPI()
app.add_middleware(
    SessionMiddleware,
    secret_key="SUPER_SECRET_KEY_123",
    same_site="lax",
    https_only=False
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# --- Named loggers ---
tick_log = logging.getLogger("TICKS")
ws_log   = logging.getLogger("WS")
lad_log  = logging.getLogger("LADDER")

def _supports_ansi() -> bool:
    try:
        return sys.stdout.isatty()
    except Exception:
        return False

def _c(text: str, code: str) -> str:
    if not _supports_ansi():
        return str(text)
    return f"\033[{code}m{text}\033[0m"

def _log_universal_settings(user_id: int, settings: Dict[str, Any]) -> None:
    msg = (
        "\n"
        + _c("╔══════════════════════════════════════════════════════╗", "90") + "\n"
        + _c("║   UNIVERSAL SETTINGS SAVED                           ║", "1;96") + "\n"
        + _c("╠══════════════════════════════════════════════════════╣", "90") + "\n"
        + _c(f"║ User ID : {str(user_id):<43}║", "97") + "\n"
        + _c(f"║ QtyMode : {str(settings.get('qty_mode', '-')):<43}║", "93") + "\n"
        + _c(f"║ Capital : {str(settings.get('per_trade_capital', '-')):<43}║", "92") + "\n"
        + _c(f"║ Qty     : {str(settings.get('per_trade_qty', '-')):<43}║", "92") + "\n"
        + _c(f"║ Thresh% : {str(settings.get('threshold_pct', '-')):<43}║", "94") + "\n"
        + _c(f"║ SL%     : {str(settings.get('stop_loss_pct', '-')):<43}║", "91") + "\n"
        + _c(f"║ TSL%    : {str(settings.get('trailing_sl_pct', '-')):<43}║", "95") + "\n"
        + _c(f"║ Cycles  : {str(settings.get('ladder_cycles', '-')):<43}║", "96") + "\n"
        + _c(f"║ MaxAdds : {str(settings.get('max_adds_per_leg', '-')):<43}║", "96") + "\n"
        + _c(f"║ MaxTrade: {str(settings.get('max_trades_per_symbol', '-')):<43}║", "1;33") + "\n"
        + _c("╚══════════════════════════════════════════════════════╝", "90")
    )
    logging.getLogger("SETTINGS").info(msg)

# Removed invalid include_router

# ------------------------------------------------
# ✅ SILENCE NOISY LOGS
# ------------------------------------------------
class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Filter out health/status checks to keep terminal clean
        return record.getMessage().find("/api/zerodha-status") == -1

# Filter uvicorn access logs
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

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
async def api_ws_tokens(user_id: int = Query(...)):
    """⚡ Worker-only endpoint
    NO session overhead.
    NO request object parsing."""
    tokens = WS_TOKENS_RAM.get(user_id, set())
    return {
        "user_id": user_id,
        "tokens": sorted(list(tokens))
    }


@app.get("/api/ws-tokens/session")
async def api_ws_tokens_session(request: Request):
    """
    🌐 Browser-only endpoint
    Uses session safely.
    """
    uid = request.session.get("user_id")
    if not uid:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    tokens = WS_TOKENS_RAM.get(int(uid), set())
    return {
        "user_id": uid,
        "tokens": sorted(list(tokens))
    }


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
            opn_raw = t.get("open", 0)
            opn  = float(opn_raw) if opn_raw not in (None, "") else 0.0

            out[sym] = {
                "symbol": sym,
                "ltp": ltp,
                "tbq": int(t.get("tbq", 0) or 0),
                "tsq": int(t.get("tsq", 0) or 0),
                "volume": int(t.get("volume", 0) or 0),
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
                    kite = await get_kite_for_user(int(uid))
                    if not kite: continue
                except:
                    continue
                # Kite quote supports multiple instruments  We fetch only those missing/expired
                to_fetch = []
                for sym in list(syms)[:200]:
                    c = await get_cached_circuit(sym)
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
                            await r.setex(f"circuit:{sym}", CIRCUIT_TTL_SEC, json.dumps(payload))
                    except:
                        continue


        except Exception:
            continue

@app.on_event("startup")
async def _startup_tasks():
    asyncio.create_task(circuit_prefetch_loop(), name="circuit_prefetch_loop")
    asyncio.create_task(auto_squareoff_loop(), name="auto_squareoff_loop")
    asyncio.create_task(daily_reset_loop(), name="daily_reset_loop")


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
                c = await get_cached_circuit(sym)
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
            msg_type = msg.get("type")
            eng = ENGINE_HUB.get(uid)

            # ------------------------------------------------
            # 1️⃣ HANDLE TICKS
            # ------------------------------------------------
            if msg_type == "ticks":
                ticks = msg.get("ticks") or []
                if not ticks:
                    continue
                
                ws_log.debug("📦 Received %d ticks user=%s", len(ticks), uid)
                
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
                    vol = t.get("volume") or t.get("volume_traded") or t.get("volumeTraded") or 0
                    try:
                        tbq = int(tbq or 0)
                        tsq = int(tsq or 0)
                        vol = int(vol or 0)
                    except:
                        tbq, tsq, vol = 0, 0, 0

                    # -------- normalize ohlc ----------
                    ohlc = t.get("ohlc") or {}
                    op   = t.get("open") or ohlc.get("open")
                    hi   = t.get("high") or ohlc.get("high")
                    lo   = t.get("low")  or ohlc.get("low")
                    prev = t.get("prev") or ohlc.get("close")

                    op   = float(op) if op not in (None, "") else 0.0
                    hi   = float(hi or ltp)
                    lo   = float(lo or ltp)
                    prev = float(prev or ltp)

                    # Keep/fetch real open if tick payload doesn't carry it.
                    if op <= 0 and sym:
                        try:
                            old_tick = TICK_HUB.ticks.get(uid, {}).get(token, {})
                            old_open = float(old_tick.get("open") or 0)
                            if old_open > 0:
                                op = old_open
                            else:
                                op = await get_cached_open_price(uid, sym)
                        except Exception:
                            op = 0.0

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
                        "volume": vol,
                        "prev": prev, "open": op, "high": hi, "low": lo,
                        "symbol": sym, "token": token
                    })

                    # ---------- LADDER ----------
                    if eng:
                        try:
                            await eng.ingest_tick(token, ltp)
                        except Exception:
                            lad_log.exception("Ladder ingest failed")

                    # ---------- DASHBOARD BROADCAST ----------
                    if sym:
                        c = await get_cached_circuit(sym)
                        upper = c.get("upper")
                        lower = c.get("lower")
                        await TICK_HUB.broadcast(uid, {
                            "symbol": sym,
                            "ltp": ltp,
                            "tbq": tbq,
                            "tsq": tsq,
                            "volume": vol,
                            "prev": prev,
                            "open": op,     
                            "high": hi,
                            "low": lo,
                            "upper_circuit": upper,  
                            "lower_circuit": lower,
                        })

            # ------------------------------------------------
            # 2️⃣ HANDLE ORDER UPDATE
            # ------------------------------------------------
            elif msg_type == "order_update":
                # ✅ Handle ORDER UPDATES (Rejections, Fills)
                payload = msg.get("payload")
                if eng and payload:
                    asyncio.create_task(eng.handle_order_update(payload))
    except Exception:
        ws_log.exception("❌ INGEST WS crashed user=%s", uid)

    finally:
        pass


# ------------------------------------------------
# Ladder Engine Hub (per-user)
# ------------------------------------------------
ENGINE_HUB: Dict[int, LadderEngine] = {}

async def get_kite_for_user(user_id: int):
    user_id = int(user_id)
    keys = await r.hgetall(f"zerodha:keys:{user_id}")
    if not keys: return None
    
    token = await r.get(f"access_token:{user_id}")
    if not token: return None

    kite = KiteConnect(api_key=keys.get("api_key"))
    kite.set_access_token(token)
    return kite

async def get_engine_for_user(user_id: int):
    user_id = int(user_id)
    eng = ENGINE_HUB.get(user_id)
    if eng is not None:
        # Ensure poller/order worker is alive; otherwise ingest_tick will ignore ticks.
        if not getattr(eng, "_running", False):
            await eng.start()
        return eng

    # Get credentials
    keys = await r.hgetall(f"zerodha:keys:{user_id}")
    if not keys: return None
    
    token = await r.get(f"access_token:{user_id}")
    if not token: return None

    # Create engine
    redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    eng = LadderEngine(
        redis_url=redis_url,
        user_id=user_id,
        api_key=keys.get("api_key"),
        access_token=token
    )
    await eng.start()
    ENGINE_HUB[user_id] = eng
    return eng

from dotenv import load_dotenv
import os

load_dotenv()  # 🔥 this loads .env into os.environ

# ------------------------------------------------
# REDIS
REDIS_URL = os.getenv("REDIS_URL")
if REDIS_URL:
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
else:
    r = aioredis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)


# ============================================================
# ✅ CIRCUIT CACHE (Redis + in-memory TTL)
# ============================================================
CIRCUIT_RAM: Dict[str, Dict[str, Any]] = {}   # symbol -> {"upper":..,"lower":..,"ts":..}
CIRCUIT_TTL_SEC = 30 * 60                     # 30 min (same day ok)
OPEN_RAM: Dict[str, Dict[str, Any]] = {}      # symbol -> {"open":..,"ts":..}
OPEN_TTL_SEC = 12 * 60 * 60                   # cache open price for trading day window

def _now_ist():
    ist = pytz.timezone("Asia/Kolkata")
    return datetime.datetime.now(ist)

def _is_market_time_for_circuit():
    # circuit values once/day, but we keep it simple
    now = _now_ist()
    if now.weekday() >= 5:
        return False
    return True


async def get_cached_circuit(sym: str) -> Dict[str, Any]:
    """
    Fast path: RAM -> Redis.
    Returns dict: {"upper": float|None, "lower": float|None, "ts": epoch}
    """
    sym = sym.upper().strip()
    if not sym:
        return {}

    # 1) RAM cache
    c = CIRCUIT_RAM.get(sym)
    if c and (time.time() - float(c.get("ts", 0) or 0)) < CIRCUIT_TTL_SEC:
        return c

    # 2) Redis cache
    raw = await r.get(f"circuit:{sym}")
    if raw:
        try:
            d = json.loads(raw)
            # keep in RAM too
            CIRCUIT_RAM[sym] = d
            return d
        except:
            pass

    return {}


async def get_cached_open_price(user_id: int, sym: str) -> float:
    """
    Fetch and cache day's open price.
    Priority: RAM -> Redis -> Kite quote.
    """
    sym = (sym or "").upper().strip()
    if not sym:
        return 0.0

    now_ts = time.time()

    # 1) RAM cache
    c = OPEN_RAM.get(sym)
    if c and (now_ts - float(c.get("ts", 0) or 0)) < OPEN_TTL_SEC:
        try:
            return float(c.get("open") or 0.0)
        except Exception:
            return 0.0

    # 2) Redis cache (date-scoped)
    day = _now_ist().strftime("%Y%m%d")
    redis_key = f"open:{day}:{sym}"
    raw = await r.get(redis_key)
    if raw not in (None, ""):
        try:
            val = float(raw)
            if val > 0:
                OPEN_RAM[sym] = {"open": val, "ts": now_ts}
                return val
        except Exception:
            pass

    # 3) Kite quote fallback
    try:
        kite = await get_kite_for_user(int(user_id))
        if not kite:
            return 0.0
        q = kite.quote([f"NSE:{sym}"]) or {}
        row = q.get(f"NSE:{sym}", {}) or {}
        ohlc = row.get("ohlc") or {}
        val = float(ohlc.get("open") or row.get("open") or 0.0)
        if val > 0:
            OPEN_RAM[sym] = {"open": val, "ts": now_ts}
            await r.set(redis_key, str(val), ex=OPEN_TTL_SEC)
            return val
    except Exception:
        return 0.0

    return 0.0



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
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    # Single-user mode: pin user_id to 1 if not set
    if "user_id" not in request.session:
        request.session["user_id"] = 1
        request.session["username"] = "User1"

    user_id = request.session["user_id"]          # int
    username = request.session.get("username", "")
    print("SESSION:", dict(request.session))

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

    await r.hset(f"zerodha:keys:{user}", "api_key", str(api_key))
    await r.hset(f"zerodha:keys:{user}", "api_secret", str(api_secret))

    return {"status": "saved"}

# ------------------------------------------------
# AUTOMATION MODE PREFERENCE
# ------------------------------------------------
@app.post("/api/automation-mode")
async def save_automation_mode(request: Request):
    """Save user's automation mode preference (MANUAL/AUTO)"""
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    
    data = await request.json()
    mode = (data.get("mode") or "MANUAL").strip().upper()
    
    if mode not in ("MANUAL", "AUTO"):
        return JSONResponse({"error": "mode must be MANUAL or AUTO"}, 400)
    
    await r.set(f"automation:mode:{user_id}", mode)
    return {"status": "saved", "mode": mode}

@app.get("/api/automation-mode")
async def get_automation_mode(request: Request):
    """Retrieve user's automation mode preference"""
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    
    mode = await r.get(f"automation:mode:{user_id}")
    return {"mode": mode or "MANUAL"}

# ------------------------------------------------
# UNIVERSAL LADDER SETTINGS
# ------------------------------------------------
@app.post("/api/universal-settings")
async def save_universal_settings(request: Request):
    """Save universal ladder settings for user"""
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    
    data = await request.json()
    settings = data.get("settings", {})
    
    # Validate required fields
    required = ["qty_mode", "per_trade_capital", "per_trade_qty", "threshold_pct", 
                "stop_loss_pct", "trailing_sl_pct", "ladder_cycles", "max_adds_per_leg", "max_trades_per_symbol"]
    
    for field in required:
        if field not in settings:
            return JSONResponse({"error": f"Missing field: {field}"}, 400)
    
    _log_universal_settings(int(user_id), settings)
    await r.set(f"universal:settings:{user_id}", json.dumps(settings))
    return {"status": "saved"}


@app.get("/api/universal-settings")
async def get_universal_settings(request: Request):
    """Retrieve universal ladder settings"""
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    
    raw = await r.get(f"universal:settings:{user_id}")
    if raw:
        try:
            settings = json.loads(raw)

            # ✅ Merge with defaults in case of new fields (like max_trades_per_symbol)
            defaults = {
                "qty_mode": "CAPITAL", "per_trade_capital": 1000, "per_trade_qty": 1,
                "threshold_pct": 1.0, "stop_loss_pct": 1.0, "trailing_sl_pct": 1.0,
                "ladder_cycles": 1, "max_adds_per_leg": 2, "max_trades_per_symbol": 1
            }
            # Update defaults with stored settings (so stored takes precedence)
            defaults.update(settings)
            
            return {"settings": defaults}
        except:
            pass
    
    # Return default values
    return {
        "settings": {
            "qty_mode": "CAPITAL",
            "per_trade_capital": 1000,
            "per_trade_qty": 1,
            "threshold_pct": 1.0,
            "stop_loss_pct": 1.0,
            "trailing_sl_pct": 1.0,
            "ladder_cycles": 1,
            "max_adds_per_leg": 2,
            "max_trades_per_symbol": 1
        }
    }

# ------------------------------------------------
# STOCK-WISE LADDER SETTINGS
# ------------------------------------------------
@app.post("/api/stock-settings")
async def save_stock_settings(request: Request):
    """Save stock-specific ladder settings"""
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    
    data = await request.json()
    symbol = (data.get("symbol") or "").strip().upper()
    settings = data.get("settings", {})
    
    if not symbol:
        return JSONResponse({"error": "Symbol required"}, 400)
    
    await r.set(f"stock:settings:{user_id}:{symbol}", json.dumps(settings))
    return {"status": "saved", "symbol": symbol}


@app.get("/api/stock-settings")
async def get_stock_settings(request: Request, symbol: str):
    """Retrieve stock-specific settings for a symbol"""
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    
    symbol = symbol.strip().upper()
    raw = await r.get(f"stock:settings:{user_id}:{symbol}")
    
    if raw:
        try:
            settings = json.loads(raw)

            return {"symbol": symbol, "settings": settings}
        except:
            pass
    
    return {"symbol": symbol, "settings": None}

@app.get("/api/stock-settings/list")
async def list_stock_settings(request: Request):
    """List all configured stock-wise settings for user"""
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    
    pattern = f"stock:settings:{user_id}:*"
    keys = await r.keys(pattern)
    
    result = []
    for key in keys:
        symbol = key.split(":")[-1] if isinstance(key, str) else key.decode().split(":")[-1]
        raw = await r.get(key)
        if raw:
            try:

                settings = json.loads(raw)
                result.append({"symbol": symbol, "settings": settings})
            except:
                continue
    
    return {"stocks": result}

@app.delete("/api/stock-settings")
async def delete_stock_settings(request: Request):
    """Delete stock-specific settings"""
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    
    data = await request.json()
    symbol = (data.get("symbol") or "").strip().upper()
    
    if not symbol:
        return JSONResponse({"error": "Symbol required"}, 400)
    
    await r.delete(f"stock:settings:{user_id}:{symbol}")
    return {"status": "deleted", "symbol": symbol}



# ------------------------------------------------
# ZERODHA LOGIN (Kite)
# ------------------------------------------------
@app.get("/connect/zerodha")
async def connect_zerodha(request: Request):
    user = request.session.get("user_id")
    if not user:
        return RedirectResponse("/")

    keys = await r.hgetall(f"zerodha:keys:{user}")
    if not keys:
        return RedirectResponse("/dashboard?error=nokeys")

    kite = KiteConnect(api_key=keys["api_key"])
    print("🔗🔗 Redirecting to Zerodha 🔗🔗")
    return RedirectResponse(kite.login_url())


# -------------------------------------------------
# ZERODHA CALLBACK
# -------------------------------------------------
@app.get("/zerodha/callback")
async def zerodha_callback(
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
    keys = await r.hgetall(f"zerodha:keys:{user_id}")
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

    await r.setex(f"access_token:{user_id}", TTL, access_token)
    await r.setex(f"api_key:{user_id}", TTL, keys["api_key"])

    # 6️⃣ DEBUG CONFIRMATION (VERY IMPORTANT)
    ttl_val = await r.ttl(f"access_token:{user_id}")
    print("✅ access_token stored 24h user:", user_id, "ttl:", ttl_val)
    print("✅ Zerodha connected for user:", user_id)
    return RedirectResponse("/dashboard")




@app.get("/api/zerodha-status")
async def zerodha_status(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return {"connected": False}
    
    keys = await r.hgetall(f"zerodha:keys:{user_id}")
    api_key = (keys or {}).get("api_key")
    access_token = await r.get(f"access_token:{user_id}")

    if api_key and access_token:
        ttl = await r.ttl(f"access_token:{user_id}")
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


@app.get("/api/circuit/{symbol}")
async def get_circuit(symbol: str):
    raw = await r.get(f"circuit:{symbol.upper()}")
    return json.loads(raw) if raw else {}



# ------------------------------------------------
# CHARTINK WEBHOOK 
# ------------------------------------------------
@app.post("/webhook/chartink")
async def chartink_webhook(request: Request):
    # Accept both JSON and form payloads from Chartink/webhook providers.
    data = {}
    try:
        data = await request.json()
    except Exception:
        try:
            form = await request.form()
            data = dict(form)
        except Exception:
            data = {}

    # Some providers wrap actual payload in a JSON string field.
    for nested_key in ("message", "payload", "data"):
        nested_raw = data.get(nested_key)
        if isinstance(nested_raw, str):
            try:
                nested_obj = json.loads(nested_raw)
                if isinstance(nested_obj, dict):
                    merged = dict(data)
                    merged.update(nested_obj)
                    data = merged
                    break
            except Exception:
                pass

    ws_log.info("Chartink webhook hit | content-type=%s", request.headers.get("content-type"))

    # Backward compatible default (existing behavior was fixed to user 1).
    # If sender passes user_id, route alerts to that user.
    try:
        user_id = int(data.get("user_id") or request.query_params.get("user_id") or 1)
    except Exception:
        user_id = 1
    ws_log.info("Chartink user_id resolved | user=%s | query=%s | data_keys=%s", user_id, dict(request.query_params), list(data.keys()))

    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.datetime.now(ist)
    if now.weekday() >= 5:
        return {"status": "ignored", "reason": "Weekend"}

    scan_name = data.get("scan_name") or data.get("scan") or "Chartink"
    stocks_raw = (
        data.get("stocks")
        or data.get("symbols")
        or data.get("symbol")
        or data.get("nsecode")
        or ""
    )
    ws_log.info("Chartink payload | scan=%s | stocks_raw_type=%s", scan_name, type(stocks_raw).__name__)

    if isinstance(stocks_raw, (list, tuple)):
        raw_symbols = [str(x) for x in stocks_raw]
    else:
        raw_symbols = [p for p in re.split(r"[\n,;]+", str(stocks_raw)) if p and p.strip()]

    # Fallback: if no direct stocks field, try extracting symbols from payload text.
    if not raw_symbols:
        haystack = " ".join(str(v) for v in data.values() if v is not None)
        candidates = re.findall(r"(?:NSE:|BSE:)?[A-Za-z][A-Za-z0-9_.-]{1,24}", haystack)
        for c in candidates:
            s = str(c).strip().upper()
            if s.startswith("NSE:") or s.startswith("BSE:"):
                s = s[4:]
            if not s:
                continue
            # Prefer valid instrument symbols; if map unavailable, keep candidate.
            if not GLOBAL_SYMBOL_TOKEN or s in GLOBAL_SYMBOL_TOKEN:
                raw_symbols.append(s)

    today = now.strftime("%Y-%m-%d")
    redis_key = f"chartink_alerts:{user_id}:{today}"
    seen_key = f"chartink_seen:{user_id}:{today}:{scan_name}"

    parsed_symbols = []
    new_stocks = []

    ws_log.info("Chartink raw symbols | count=%s | sample=%s", len(raw_symbols), raw_symbols[:10])

    for s in raw_symbols:
        symbol = str(s).strip().upper().strip("'\"")
        if symbol.startswith("NSE:"):
            symbol = symbol[4:]
        if symbol.startswith("BSE:"):
            symbol = symbol[4:]
        if not symbol:
            continue
        parsed_symbols.append(symbol)

        if await r.sismember(seen_key, symbol):
            continue

        # ✅ mark seen
        await r.sadd(seen_key, symbol)

        new_stocks.append(symbol)

        # 🔥 TOKEN ENSURE
        tok = SYMBOL_TOKEN_RAM.get(user_id, {}).get(symbol) or GLOBAL_SYMBOL_TOKEN.get(symbol)
        if tok:
            add_ws_token(user_id, int(tok))
        else:
            ws_log.debug("Chartink token missing | user=%s | symbol=%s", user_id, symbol)

    if not parsed_symbols:
        ws_log.warning("Chartink webhook ignored: no symbols in payload | user=%s keys=%s", user_id, list(data.keys()))
        return {"status": "ignored", "reason": "no_symbols"}

    # If all were already seen today, still push for dashboard visibility.
    display_stocks = new_stocks if new_stocks else parsed_symbols

    packet = {
        "id": int(time.time() * 1000),
        "scan": scan_name,
        "stocks": display_stocks,
        "time": now.strftime("%H:%M:%S")
    }
    ws_log.info("Chartink packet | user=%s | scan=%s | parsed=%s | new=%s | display=%s", user_id, scan_name, len(parsed_symbols), len(new_stocks), len(display_stocks))
    ws_log.info(f"📥📩 Chartink webhook received 📥📩 -→ {packet}")
    
    # 🔴 Await Redis calls
    await r.lpush(redis_key, json.dumps(packet))
    await r.ltrim(redis_key, 0, 200)
    ws_log.info("Chartink stored | key=%s | list_len=%s", redis_key, await r.llen(redis_key))

    return {"status": "success"}

# ------------------------------------------------
# GET ALERTS (Dashboard)
# ------------------------------------------------
@app.get("/api/alerts")
async def get_alerts(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        ws_log.warning("GET /api/alerts blocked: no session user_id")
        return []

    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.datetime.now(ist).strftime("%Y-%m-%d")

    redis_key = f"chartink_alerts:{user_id}:{today}"
    raw = await r.lrange(redis_key, 0, -1)
    ws_log.info("GET /api/alerts | user=%s | key=%s | count=%s", user_id, redis_key, len(raw))

    # Backward compatibility: webhook may have been writing to user 1.
    if not raw and int(user_id) != 1:
        fallback_key = f"chartink_alerts:1:{today}"
        raw = await r.lrange(fallback_key, 0, -1)
        ws_log.info("GET /api/alerts fallback | key=%s | count=%s", fallback_key, len(raw))

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
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)
    
    symbol = body.symbol.strip().upper()
    if not symbol:
        return JSONResponse({"error": "Symbol missing"}, 400)

    # 1?? Try to use LADDER ENGINE first (Best for state consistency & stopping re-entry)
    eng = ENGINE_HUB.get(int(user_id))
    if eng:
        # Check if session exists (active or not)
        sess = eng.sessions.get(symbol)
        if sess and sess.active:
            # Delegate to engine: Stops session, Exit order, Updates state
            res = await eng.squareoff_symbol(symbol, reason="MANUAL_BTN")
            return res

    # 2?? Fallback: Direct Zerodha Squareoff (if engine lost track or not active)
    kite = await get_kite_for_user(int(user_id))
    if not kite:
        return JSONResponse({"error": "Kite not connected"}, 401)
    
    pos = kite.positions().get("net", [])

    net_qty = 0
    for p in pos:
        if str(p.get("tradingsymbol", "")).upper() == symbol:
            net_qty = int(p.get("quantity", 0) or 0)
            break
            
    if net_qty == 0:
        # Keep ladder entry for current day (mark complete)
        key = f"ladder:state:{user_id}:{symbol}"
        raw = await r.get(key)
        if raw:
            try:
                data = json.loads(raw)
                data["active"] = False
                data.setdefault("session_day", datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d"))
                if data.get("leg_state"):
                    data["leg_state"]["status"] = "COMPLETE"
                    data["leg_state"]["reason"] = "NO_POSITION"
                await r.set(key, json.dumps(data))
            except Exception:
                pass
        # Also ensure memory cleanup
        if eng and hasattr(eng, "sessions") and symbol in eng.sessions:
             eng.sessions.pop(symbol, None)
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
    #?? Keep ladder state after placing exit (mark complete)
    key = f"ladder:state:{user_id}:{symbol}"
    raw = await r.get(key)
    if raw:
        try:
            data = json.loads(raw)
            data["active"] = False
            data.setdefault("session_day", datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d"))
            if data.get("leg_state"):
                data["leg_state"]["status"] = "COMPLETE"
                data["leg_state"]["reason"] = "SQUAREOFF_PLACED"
            await r.set(key, json.dumps(data))
        except Exception:
            pass
    return {"status": "squareoff_placed", "symbol": symbol, "qty": qty, "order_id": oid}@app.get("/api/ladder/sessions")
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

        # 2️⃣ Merge Redis snapshots (keep completed for the day)
        keys = await r.keys(f"ladder:state:{user_id}:*")
        for k in keys:
            raw = await r.get(k)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            sym = str(data.get("symbol", "")).upper()
            if not sym:
                continue
            if not any(str(x.get("symbol", "")).upper() == sym for x in out):
                out.append(data)

        # Filter: keep only current day
        today = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d")
        filtered = []
        for s in out:
            day = s.get("session_day")
            if day and day != today:
                continue
            filtered.append(s)

        return JSONResponse(filtered)
    except Exception as e:
        print("❌ /api/ladder/state error:", e)
        return JSONResponse([], status_code=500)




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
        kite = await get_kite_for_user(int(user_id))
        if not kite:
             return JSONResponse({"error": "Kite not connected"}, 401)
             
        pos = kite.positions().get("net", [])
        net_qty = 0
        for p in pos:
            if str(p.get("tradingsymbol", "")).upper() == symbol:
                net_qty = int(p.get("quantity", 0) or 0)
                break
        if net_qty == 0:
        # Keep ladder entry for current day (mark complete)
            key = f"ladder:state:{user_id}:{symbol}"
            raw = await r.get(key)
            if raw:
                try:
                    data = json.loads(raw)
                    data["active"] = False
                    data.setdefault("session_day", datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d"))
                    if data.get("leg_state"):
                        data["leg_state"]["status"] = "COMPLETE"
                        data["leg_state"]["reason"] = "NO_POSITION"
                    await r.set(key, json.dumps(data))
                except Exception:
                    pass
    # Also ensure memory cleanup
            eng = ENGINE_HUB.get(int(user_id))
            if eng and hasattr(eng, "sessions") and symbol in eng.sessions:
                eng.sessions.pop(symbol, None)
            return {"status": "no_position", "symbol": symbol}     
        # ✅ PLACE SQUAREOFF ORDER   
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

# ────────────────────────────────────────────────────────────────
#               CORE SQUARE-OFF LOGIC (Standalone)
# ────────────────────────────────────────────────────────────────
async def execute_squareoff_all(user_id: int):
    """
    Core function to square off all positions for a user.
    Can be called by API route OR background scheduler.
    """
    user_id = int(user_id)
    logging.info(f"Starting Square-off all positions for user {user_id}")

    try:
        kite = await get_kite_for_user(user_id)
        if not kite:
             return {"error": "Kite not connected"}
             
        pos = kite.positions().get("net", [])


        placed = []
        symbols_to_cleanup = set()

        # ✅ CRITICAL FIX: Also include any active sessions from RAM, 
        # even if Zerodha has no open position (e.g. IDLE / WAIT_FEED state)
        eng = ENGINE_HUB.get(user_id)
        if eng and hasattr(eng, "sessions"):
            for s in eng.sessions.keys():
                symbols_to_cleanup.add(s)

        # 1) Place squareoff orders for all open positions
        for p in pos:
            sym = str(p.get("tradingsymbol", "")).upper()
            q = int(p.get("quantity", 0) or 0)
            if not sym or q == 0:
                continue

            side = kite.TRANSACTION_TYPE_SELL if q > 0 else kite.TRANSACTION_TYPE_BUY
            
            try:
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
                logging.info(f"Square-off order placed: {sym} | qty: {abs(q)} | oid: {oid}")
            except Exception as e:
                # Log error but CONTINUE cleanup
                logging.error(f"Square-off order failed for {sym} user {user_id}: {e}")
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

        # 3) Keep ladder state for the day (mark complete) + clear locks
        del_keys = []
        today = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d")
        for sym in symbols_to_cleanup:
            key = f"ladder:state:{user_id}:{sym}"
            raw = await r.get(key)
            if raw:
                try:
                    data = json.loads(raw)
                    data["active"] = False
                    data.setdefault("session_day", today)
                    if data.get("leg_state"):
                        data["leg_state"]["status"] = "COMPLETE"
                        data["leg_state"]["reason"] = "SQUAREOFF_ALL"
                    await r.set(key, json.dumps(data))
                except Exception:
                    pass
            del_keys.extend([
                f"ladder:lock:{user_id}:{sym}:entry",
                f"ladder:lock:{user_id}:{sym}:add",
                f"ladder:lock:{user_id}:{sym}:exit",
            ])
        if del_keys:
            await r.delete(*del_keys)

        # 4) Optional: remove tokens from ws set (if you want WS to stop tracking these)
        # If you want this, only remove tokens for symbols cleanup.
        # (depends on how you manage symbol->token mapping)
        for sym in symbols_to_cleanup:
            token = await r.get(f"symbol_token:{sym}")
            if token and str(token).isdigit():
                await r.srem(f"ws:{user_id}:tokens", int(token))

        # 5) Reset Trade Counts (Server-Side) so "Fresh Auto-Start" works seamlessly
        # This allows re-trading same symbols if user explicitly resets via Kill Switch
        count_keys = await r.keys(f"ladder:count:{user_id}:*")
        if count_keys:
             await r.delete(*count_keys)
        
        # Clear Global Count
        await r.delete(f"ladder:global_count:{user_id}")

        return {"status": "Square-off completed", "count": len(placed), "orders": placed}

    except Exception as e:
        # Rethrow or return error dict so caller handles it
        logging.error(f"Square-off all execution failed: {e}")
        return {"error": str(e)}


@app.post("/api/squareoff_all")  # Exit all
async def api_squareoff_all(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)

    result = await execute_squareoff_all(int(user_id))
    if "error" in result:
        return JSONResponse(result, 500)
    return result


@app.get("/api/auto-squareoff-event")
async def api_auto_squareoff_event(request: Request):
    """
    Dashboard poll endpoint:
    Returns latest AUTO square-off trigger info for logged-in user.
    """
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, 401)

    key = f"auto_squareoff:event:{int(user_id)}"
    raw = await r.get(key)
    if not raw:
        return {"triggered": False}

    try:
        payload = json.loads(raw)
    except Exception:
        return {"triggered": False}

    return {
        "triggered": True,
        "ts": payload.get("ts"),
        "time_ist": payload.get("time_ist"),
        "count": int(payload.get("count", 0)),
        "status": payload.get("status", "SUCCESS"),
    }

# ────────────────────────────────────────────────────────────────
#               AUTO SQUARE-OFF AT 3:20 PM IST
# ────────────────────────────────────────────────────────────────
async def auto_squareoff_loop():
    """
    Background task: checks every 60 seconds.
    Triggers square-off at exactly 15:20 IST on weekdays.
    """
    triggered_today = None  # Track if already triggered today
    
    while True:
        now = _now_ist()
        today_date = now.date()
        
        # Skip weekends
        if now.weekday() >= 5:
            await asyncio.sleep(60)
            continue

        # ✅ Check if time is 15:20 and not triggered yet today
        if now.hour == 15 and now.minute == 20 and triggered_today != today_date:
            logging.info("⏰ AUTO SQUARE-OFF TRIGGERED at 15:20 IST")

            triggered_today = today_date  # Mark as triggered for today

            cursor = 0
            while True:
                cursor, keys = await r.scan(cursor=cursor, match="access_token:*", count=100)
                for key in keys:
                    try:
                        uid_str = key.decode().split(":")[-1] if isinstance(key, bytes) else key.split(":")[-1]
                        if not uid_str.isdigit():
                            continue
                        user_id = int(uid_str)

                        # Only process if token still exists
                        if await r.exists(key):
                            # ✅ FIXED: Call extracted function, NOT the route handler
                            sq_res = await execute_squareoff_all(user_id)
                            if "error" not in sq_res:
                                evt_key = f"auto_squareoff:event:{user_id}"
                                evt_payload = {
                                    "ts": int(time.time()),
                                    "time_ist": now.strftime("%H:%M:%S"),
                                    "count": int(sq_res.get("count", 0)),
                                    "status": "SUCCESS",
                                }
                                await r.set(evt_key, json.dumps(evt_payload), ex=60 * 60 * 12)
                    except Exception as e:
                        logging.error(f"Error processing auto square-off for key {key}: {e}")

                if cursor == 0:
                    break
            logging.info("=" * 60)
            logging.info("✅ [ AUTO SQUARE-OFF ] COMPLETED | USER=%s | 🚀 STATUS=SUCCESS", user_id)
            logging.info("=" * 60)
        
        # Sleep for 1 second (check frequently for precision)
        await asyncio.sleep(1)


async def _delete_keys_by_patterns(patterns: List[str]) -> int:
    total = 0
    for pattern in patterns:
        cursor = 0
        while True:
            cursor, keys = await r.scan(cursor=cursor, match=pattern, count=500)
            if keys:
                await r.delete(*keys)
                total += len(keys)
            if cursor == 0:
                break
    return total


async def daily_reset_loop():
    """
    Background task: clears user data at 08:00 IST daily.
    Resets credentials, settings, ladder state/locks, alerts, and WS tokens.
    """
    triggered_today = None
    patterns = [
        "zerodha:keys:*",
        "access_token:*",
        "automation:mode:*",
        "universal:settings:*",
        "ladder:settings:universal:*",
        "ladder:settings:stock:*",
        "ladder:state:*",
        "ladder:lock:*",
        "ladder:events:*",
        "ladder:count:*",
        "ladder:global_count:*",
        "chartink_alerts:*",
        "ws:*:tokens",
    ]

    while True:
        now = _now_ist()
        today_date = now.date()

        if now.hour == 8 and now.minute == 0 and triggered_today != today_date:
            logging.warning("🧹 DAILY RESET TRIGGERED at 08:00 IST")
            triggered_today = today_date
            try:
                deleted = await _delete_keys_by_patterns(patterns)
                logging.warning("✅ DAILY RESET DONE | keys_deleted=%s", deleted)
            except Exception as e:
                logging.error("❌ DAILY RESET FAILED: %s", e, exc_info=True)

        await asyncio.sleep(1)

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
        kite = await get_kite_for_user(int(user_id))
        if not kite:
             return JSONResponse({"error": "Kite not connected"}, 401)

        # ✅ Price band check (user request): allow only 100–4000 LTP
        try:
            q = kite.quote(f"NSE:{symbol}")
            ltp = float(q.get(f"NSE:{symbol}", {}).get("last_price") or 0)
        except Exception:
            ltp = 0

        if ltp <= 0:
            return JSONResponse({"error": "Unable to fetch LTP for price-band check"}, 400)
        if ltp < 100 or ltp > 4000:
            return JSONResponse({"error": f"Price outside allowed range (100-4000): {ltp:.2f}"}, 400)

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

    # 1) delete ladder state + locks + stock settings (ASYNC redis calls)
    await r.delete(
        f"ladder:state:{user_id}:{symbol}",
        f"ladder:lock:{user_id}:{symbol}:entry",
        f"ladder:lock:{user_id}:{symbol}:add",
        f"ladder:lock:{user_id}:{symbol}:exit",
        f"ladder:settings:stock:{user_id}:{symbol}",
    )

    # 2) remove token from ws set (control set)
    token = await r.get(f"symbol_token:{symbol}")
    if token and str(token).isdigit():
        await r.srem(f"ws:{user_id}:tokens", int(token))

    # 3) also remove in-memory ladder session (IMPORTANT)
    eng = ENGINE_HUB.get(int(user_id))
    if eng:
        try:
            # we'll implement this method in ladder_engine.py next:
            await eng.remove_session(symbol)
        except Exception:
            pass

    return {"status": "deleted", "symbol": symbol}




