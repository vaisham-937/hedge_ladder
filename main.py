from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from starlette.middleware.sessions import SessionMiddleware
from passlib.context import CryptContext
from kiteconnect import KiteConnect
from pydantic import BaseModel
import asyncio
from typing import Dict, Optional, Any, List
from ladder_engine import LadderEngine, LadderSettings
import redis.asyncio as aioredis
import json, time, datetime, pytz, os, re
import subprocess
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

async def _run_fetch_circuit_job(user_id: int) -> None:
    try:
        logging.info("🗓️ FETCH_CIRCUIT START | user=%s | ts=%s", user_id, _now_ist().strftime("%H:%M:%S"))
        cmd = [sys.executable, "fetch_nse_cash_instruments.py", "--user-id", str(user_id), "--fetch-circuit"]
        res = await asyncio.to_thread(
            subprocess.run,
            cmd,
            cwd=BASE_DIR,
            capture_output=True,
            text=True
        )
        logging.info("✅ FETCH_CIRCUIT DONE  | user=%s | rc=%s | ts=%s", user_id, res.returncode, _now_ist().strftime("%H:%M:%S"))
        if res.stdout:
            logging.info("FETCH_CIRCUIT OUT   | %s", res.stdout.strip()[:500])
        if res.stderr:
            logging.warning("FETCH_CIRCUIT ERR   | %s", res.stderr.strip()[:500])
        if res.returncode == 0:
            load_instruments_file()
            _load_circuit_file_to_ram()
    except Exception as e:
        logging.error("❌ FETCH_CIRCUIT FAIL | %s", e, exc_info=True)


async def daily_fetch_circuit_loop():
    """
    Background task: runs fetch_nse_cash_instruments.py at 09:00 IST daily.
    Same style as daily_reset_loop.
    """
    triggered_today = None
    while True:
        now = _now_ist()
        today_date = now.date()
        if now.hour == 9 and now.minute == 0 and triggered_today != today_date:
            triggered_today = today_date
            await _run_fetch_circuit_job(user_id=1)
        await asyncio.sleep(1)
def _supports_ansi() -> bool:
    return False

def _c(text: str, code: str) -> str:
    return str(text)

def _log_universal_settings(user_id: int, settings: Dict[str, Any]) -> None:
    logging.getLogger("SETTINGS").info(
        "SETTINGS SAVED user=%s qty_mode=%s capital=%s qty=%s thresh=%s sl=%s tsl=%s cycles=%s max_adds=%s max_trades=%s",
        user_id,
        settings.get("qty_mode", "-"),
        settings.get("per_trade_capital", "-"),
        settings.get("per_trade_qty", "-"),
        settings.get("threshold_pct", "-"),
        settings.get("stop_loss_pct", "-"),
        settings.get("trailing_sl_pct", "-"),
        settings.get("ladder_cycles", "-"),
        settings.get("max_adds_per_leg", "-"),
        settings.get("max_trades_per_symbol", "-"),
    )

# Removed invalid include_router

# ------------------------------------------------
# ✅ SILENCE NOISY LOGS
# ------------------------------------------------
WS_TOKENS_LOG_TS = 0.0
AUTO_RETRY_LIMIT_LOG_TS: Dict[str, float] = {}
AUTO_RETRY_LIMIT_STATE: Dict[int, Dict[str, Any]] = {}

class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Filter out health/status checks to keep terminal clean
        global WS_TOKENS_LOG_TS
        msg = record.getMessage()
        if "/api/zerodha-status" in msg:
            return False
        if "/api/ws-tokens" in msg or "ws-tokens" in msg:
            now = time.time()
            if now - WS_TOKENS_LOG_TS < 60:
                return False
            WS_TOKENS_LOG_TS = now
            return True
        # Suppress noisy WS open/close spam
        if "connection closed" in msg or "connection open" in msg:
            return False
        return True

# Filter uvicorn access logs
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

# ------------------------------------------------
# Add an in-memory Tick Hub
# ------------------------------------------------

# Suppress repeated alerts log lines (only log on change + count>0)
LAST_ALERTS_LOG = {"key": None, "head": None}

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
# (already defined above)

# ===== Instruments file (NO REDIS mapping) =====
INSTR_FILE = os.path.join(BASE_DIR, "nse_eq_instruments.json")

# ✅ OPTIONAL: Global symbol->token map from a local json file (recommended)
# Create it using fetch_nse_instruments.py => nse_eq_instruments.json
GLOBAL_SYMBOL_TOKEN: Dict[str, int] = {}
GLOBAL_TOKEN_SYMBOL: Dict[int, str] = {}
GLOBAL_SYMBOL_LIST: List[str] = []

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
        global GLOBAL_SYMBOL_LIST
        GLOBAL_SYMBOL_LIST = sorted(GLOBAL_SYMBOL_TOKEN.keys())
        ws_log.info("✅ Instruments loaded: %d symbols", len(GLOBAL_SYMBOL_TOKEN))
    except Exception as e:
        ws_log.exception("❌ Failed to load instruments json: %s", e)

load_instruments_file()




# ============================================================
# ✅ WS Tokens Control (kite_ws_worker reads this via /api/ws-tokens)
# ============================================================

WS_TOKENS_RAM: Dict[int, set] = defaultdict(set)
TIMING_TICK_SEEN: Dict[int, set] = defaultdict(set)

def add_ws_token(user_id: int, token: int):
    if token and token > 0:
        WS_TOKENS_RAM[user_id].add(int(token))

def remove_ws_token(user_id: int, token: int):
    if token and token > 0:
        WS_TOKENS_RAM[user_id].discard(int(token))

def _resolve_token(user_id: int, symbol: str) -> Optional[int]:
    symbol = (symbol or "").strip().upper()
    # Prefer RAM mapping, fallback to global instruments
    tok = SYMBOL_TOKEN_RAM.get(int(user_id), {}).get(symbol)
    if tok:
        return int(tok)
    tok = GLOBAL_SYMBOL_TOKEN.get(symbol)
    if tok:
        return int(tok)
    return None


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

async def auto_retry_loop():
    """
    Retry auto-trades that were skipped due to missing open/LTP data.
    Uses Redis pending sets: auto:pending:{user_id}:{scan_bucket}
    """
    while True:
        try:
            await asyncio.sleep(1)

            ist = pytz.timezone("Asia/Kolkata")
            now_local = datetime.datetime.now(ist)
            if now_local.weekday() >= 5:
                continue
            open_dt = now_local.replace(hour=9, minute=15, second=0, microsecond=0)
            close_dt = now_local.replace(hour=15, minute=30, second=0, microsecond=0)
            if now_local < open_dt or now_local >= close_dt:
                continue

            cursor = 0
            keys = []
            while True:
                cursor, batch = await r.scan(cursor=cursor, match="auto:pending:*", count=200)
                keys.extend(batch or [])
                if cursor == 0:
                    break

            if not keys:
                continue

            for key in keys:
                try:
                    parts = str(key).split(":")
                    if len(parts) < 4:
                        continue
                    user_id = int(parts[2])
                    scan_bucket = parts[3]
                except Exception:
                    continue

                try:
                    pending_syms = await r.smembers(key)
                except Exception:
                    pending_syms = set()
                if not pending_syms:
                    continue

                # Respect automation mode: do not auto-dispatch in MANUAL
                try:
                    mode = await r.get(f"automation:mode:{int(user_id)}")
                    if str(mode or "MANUAL").upper() != "AUTO":
                        continue
                except Exception:
                    pass

                # Block auto-retry after auto square-off (day-level halt)
                try:
                    if await r.exists(_post_squareoff_halt_key(int(user_id))):
                        try:
                            await r.delete(key)
                        except Exception:
                            pass
                        continue
                except Exception:
                    pass

                # Load universal settings payload (same as webhook)
                settings_payload = {}
                raw_settings = await r.get(f"universal:settings:{user_id}")
                if raw_settings:
                    try:
                        settings_payload = json.loads(raw_settings) or {}
                    except Exception:
                        settings_payload = {}

                entry_enabled = bool(settings_payload.get("entry_threshold_enabled", False))
                try:
                    entry_thresh = float(settings_payload.get("entry_threshold_pct", 0.0) or 0.0)
                except Exception:
                    entry_thresh = 0.0

                eligible = []
                side_by_sym = {}
                keep_pending = []
                ltp_open_cache = {}
                for sym in list(pending_syms):
                    try:
                        # Skip symbols that already completed today
                        if await _is_ladder_completed_today(int(user_id), sym):
                            continue
                        snap = TICK_HUB.get_snapshot_for_user(int(user_id), only_symbols={sym})
                        t = snap.get(sym, {}) if isinstance(snap, dict) else {}
                        ltp = float(t.get("ltp") or 0.0)
                        opn = float(t.get("open") or 0.0)
                        if opn <= 0:
                            opn = await get_cached_open_price(int(user_id), sym)
                        if opn > 0 and ltp > 0:
                            if entry_enabled and entry_thresh > 0:
                                diff_pct = ((ltp - opn) / opn) * 100.0
                                if diff_pct >= entry_thresh:
                                    eligible.append(sym)
                                    ltp_open_cache[sym] = (ltp, opn)
                                    side_by_sym[sym] = "BUY"
                                elif diff_pct <= -entry_thresh:
                                    eligible.append(sym)
                                    ltp_open_cache[sym] = (ltp, opn)
                                    side_by_sym[sym] = "SELL"
                                else:
                                    keep_pending.append(sym)
                            else:
                                eligible.append(sym)
                                ltp_open_cache[sym] = (ltp, opn)
                                side_by_sym[sym] = "BUY"
                        else:
                            keep_pending.append(sym)
                    except Exception:
                        keep_pending.append(sym)

                if eligible:
                    eng = await get_engine_for_user(int(user_id))
                    if eng:
                        # Daily total trades cap (stop all new dispatch)
                        try:
                            daily_cap = await _get_daily_trade_cap(int(user_id), 0)
                            if daily_cap > 0:
                                cur_total = await _daily_trade_count(int(user_id))
                                if cur_total >= daily_cap:
                                    # Clear pending queue for this key to avoid future dispatch
                                    try:
                                        await r.delete(key)
                                    except Exception:
                                        pass
                                    continue
                        except Exception:
                            pass
                        # Respect active positions limit (per user)
                        try:
                            max_limit = int(settings_payload.get("max_trades_per_symbol") or 0)
                        except Exception:
                            max_limit = 0
                        if max_limit > 0:
                            current_active = await _get_live_positions_count(int(user_id), fallback=_count_active_sessions(eng))
                            remaining = max(0, max_limit - current_active)
                            # Track latest limit state for UI (show when remaining=0)
                            AUTO_RETRY_LIMIT_STATE[int(user_id)] = {
                                "scan": scan_bucket,
                                "active": int(current_active),
                                "limit": int(max_limit),
                                "remaining": int(remaining),
                                "ts": float(time.time()),
                            }

                            # Throttle repetitive limit logs per user+scan (downgrade to DEBUG)
                            key = f"{user_id}:{scan_bucket}"
                            now_ts = time.time()
                            last_ts = AUTO_RETRY_LIMIT_LOG_TS.get(key, 0.0)
                            if now_ts - last_ts >= 30:
                                # log suppressed (kept for optional debug in future)
                                # ws_log.debug("AUTO_RETRY limit check | user=%s scan=%s active=%s limit=%s remaining=%s",
                                #              user_id, scan_bucket, current_active, max_limit, remaining)
                                AUTO_RETRY_LIMIT_LOG_TS[key] = now_ts
                            if current_active >= max_limit:
                                continue
                            if remaining <= 0:
                                continue
                            eligible = eligible[:remaining]
                        filtered = []
                        for sym in eligible:
                            existing = eng.sessions.get(sym)
                            if existing and existing.active:
                                continue
                            filtered.append(sym)
                        tasks = []
                        for sym in filtered:
                            ws_log.info("AUTO_RETRY dispatch | user=%s scan=%s symbol=%s", user_id, scan_bucket, sym)
                            ltp_hint, open_hint = ltp_open_cache.get(sym, (None, None))
                            side_hint = side_by_sym.get(sym, "BUY")
                            tasks.append(eng.start_ladder(
                                symbol=sym,
                                started_side=side_hint,
                                settings_mode="UNIVERSAL",
                                settings_payload=settings_payload,
                                scan_name=scan_bucket,
                                ltp_hint=ltp_hint,
                                open_hint=open_hint,
                                alert_ts=time.time(),
                                dispatch_ts=time.time(),
                            ))
                        if tasks:
                            results = await asyncio.gather(*tasks, return_exceptions=True)
                            # Increment daily trade count for successful starts
                            try:
                                for res in results:
                                    if isinstance(res, dict) and res.get("error"):
                                        continue
                                    if isinstance(res, dict) and res.get("status") in ("waiting_for_feed", "waiting_for_ltp", "started"):
                                        await _incr_daily_trade(int(user_id))
                            except Exception:
                                pass
                    else:
                        keep_pending.extend(eligible)

                # Update pending set: keep only still pending
                try:
                    await r.delete(key)
                    if keep_pending:
                        await r.sadd(key, *keep_pending)
                        await r.expire(key, 60 * 60 * 16)
                except Exception:
                    pass
        except Exception:
            continue


async def ladder_stats_loop():
    """
    Periodic ladder stats log (every 60s).
    Shows active vs exit/complete counts per user.
    """
    while True:
        try:
            await asyncio.sleep(60)
            if not ENGINE_HUB:
                continue
            for uid, eng in list(ENGINE_HUB.items()):
                try:
                    sessions = list(getattr(eng, "sessions", {}).values())
                    active_cnt = 0
                    exit_cnt = 0
                    total_cnt = 0
                    for s in sessions:
                        st = getattr(s, "leg_state", None)
                        status = str(getattr(st, "status", "") or "").upper() if st else ""
                        if status not in ("REJECTED", "ERROR", "CANCELLED", "FAILURE"):
                            total_cnt += 1
                        # Count only real live positions (qty>0) to match demat
                        if getattr(s, "active", False) and status in ("RUNNING", "EXITING"):
                            qty = int(getattr(st, "qty", 0) or 0) if st else 0
                            if qty > 0:
                                active_cnt += 1
                        if status in ("DONE", "COMPLETE"):
                            exit_cnt += 1
                    # Use Zerodha live positions count for strict matching
                    live_cnt = await _get_live_positions_count(uid, fallback=active_cnt)
                    active_cnt = live_cnt
                    total_cnt = live_cnt
                    exit_cnt = 0
                    msg = f"📊 LADDER STATS user={uid} active={active_cnt} exit={exit_cnt} total={total_cnt}"
                    lad_log.info("\033[1m%s\033[0m", msg)
                except Exception:
                    continue
        except Exception:
            continue

def _count_active_sessions(eng) -> int:
    try:
        sessions = list(getattr(eng, "sessions", {}).values())
    except Exception:
        return 0
    active_cnt = 0
    for s in sessions:
        st = getattr(s, "leg_state", None)
        status = str(getattr(st, "status", "") or "").upper() if st else ""
        if getattr(s, "active", False) and status in ("RUNNING", "IDLE", "EXITING"):
            active_cnt += 1
    return active_cnt

@app.on_event("startup")
async def _startup_tasks():
    _load_circuit_file_to_ram()
    asyncio.create_task(auto_retry_loop(), name="auto_retry_loop")
    asyncio.create_task(auto_squareoff_loop(), name="auto_squareoff_loop")
    asyncio.create_task(daily_reset_loop(), name="daily_reset_loop")
    asyncio.create_task(ladder_stats_loop(), name="ladder_stats_loop")
    asyncio.create_task(daily_fetch_circuit_loop(), name="daily_fetch_circuit_loop")


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


# -------------------------------------------------
# Background workers (per-user) for ingest processing
# -------------------------------------------------
TICK_QUEUES: Dict[int, asyncio.Queue] = {}
DASH_QUEUES: Dict[int, asyncio.Queue] = {}
ORDER_QUEUES: Dict[int, asyncio.Queue] = {}
WORKERS_STARTED: set[int] = set()


def _get_queue(qmap: Dict[int, asyncio.Queue], uid: int, maxsize: int = 2000) -> asyncio.Queue:
    q = qmap.get(uid)
    if q is None:
        q = asyncio.Queue(maxsize=maxsize)
        qmap[uid] = q
    return q


def _ensure_workers(uid: int) -> None:
    if uid in WORKERS_STARTED:
        return
    WORKERS_STARTED.add(uid)
    asyncio.create_task(_tick_worker(uid), name=f"tick_worker:{uid}")
    asyncio.create_task(_dash_worker(uid), name=f"dash_worker:{uid}")
    asyncio.create_task(_order_worker(uid), name=f"order_worker:{uid}")


async def _tick_worker(uid: int) -> None:
    q = _get_queue(TICK_QUEUES, uid)
    while True:
        t = await q.get()
        try:
            await _process_tick(uid, t)
        except Exception:
            lad_log.exception("Tick process failed user=%s", uid)
        finally:
            q.task_done()


async def _dash_worker(uid: int) -> None:
    q = _get_queue(DASH_QUEUES, uid)
    while True:
        try:
            # Batch small bursts to reduce WS chatter
            payload = await q.get()
            batch: List[Dict[str, Any]] = []
            batch.append(payload)
            t0 = time.perf_counter()
            # up to 20 ticks or 5ms window
            while len(batch) < 20 and (time.perf_counter() - t0) < 0.005:
                try:
                    nxt = q.get_nowait()
                    batch.append(nxt)
                except asyncio.QueueEmpty:
                    break
            await TICK_HUB.broadcast(uid, {"type": "ticks", "ticks": batch})
        except Exception:
            ws_log.exception("Dashboard broadcast failed user=%s", uid)
        finally:
            for _ in range(len(batch) if "batch" in locals() else 1):
                q.task_done()


async def _order_worker(uid: int) -> None:
    q = _get_queue(ORDER_QUEUES, uid)
    while True:
        payload = await q.get()
        try:
            eng = ENGINE_HUB.get(uid)
            if eng and payload:
                await eng.handle_order_update(payload)
        except Exception:
            lad_log.exception("Order update failed user=%s", uid)
        finally:
            q.task_done()


async def _process_tick(uid: int, t: Dict[str, Any]) -> None:
    # -------- normalize token/ltp ----------
    token = t.get("token") or t.get("instrument_token") or t.get("instrumentToken")
    ltp = t.get("ltp") or t.get("last_price") or t.get("lastPrice")

    try:
        token = int(token or 0)
        ltp = float(ltp or 0)
    except Exception:
        return

    if token <= 0 or ltp <= 0:
        return

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
    except Exception:
        tbq, tsq, vol = 0, 0, 0

    # -------- normalize ohlc ----------
    ohlc = t.get("ohlc") or {}
    op = t.get("open") or ohlc.get("open")
    hi = t.get("high") or ohlc.get("high")
    lo = t.get("low") or ohlc.get("low")
    prev = t.get("prev") or ohlc.get("close")

    op = float(op) if op not in (None, "") else 0.0
    hi = float(hi or ltp)
    lo = float(lo or ltp)
    prev = float(prev or ltp)

    # Keep/fetch real open if tick payload doesn't carry it. (Fast path only)
    if op <= 0 and sym:
        try:
            old_tick = TICK_HUB.ticks.get(uid, {}).get(token, {})
            old_open = float(old_tick.get("open") or 0)
            if old_open > 0:
                op = old_open
            else:
                cached = OPEN_RAM.get(sym) or {}
                cached_open = float(cached.get("open") or 0)
                if cached_open > 0:
                    op = cached_open
                else:
                    asyncio.create_task(_prefetch_open_price(uid, sym))
        except Exception:
            op = 0.0

    # ---------- TOKEN MAP (RAM) ----------
    if sym:
        SYMBOL_TOKEN_RAM[uid][sym] = token
        add_ws_token(uid, token)
        eng = ENGINE_HUB.get(uid)
        if eng:
            if getattr(eng, "SYMBOL_TOKEN_RAM", None) is None:
                eng.SYMBOL_TOKEN_RAM = {}
            eng.SYMBOL_TOKEN_RAM.setdefault(uid, {})[sym] = token

    # ---------- Timing: subscribe -> first tick ----------
    if sym:
        try:
            if sym not in TIMING_TICK_SEEN[uid]:
                tkey = f"timing:{uid}:{sym}"
                sub_ts = await r.hget(tkey, "subscribe_ts")
                tick_ts = await r.hget(tkey, "tick_ts")
                if sub_ts and not tick_ts:
                    await r.hset(tkey, mapping={"tick_ts": time.time()})
                    await r.expire(tkey, 60 * 60)
                TIMING_TICK_SEEN[uid].add(sym)
        except Exception:
            pass

    # ---------- TickHub RAM ----------
    TICK_HUB.update_tick(uid, token, {
        "ltp": ltp, "tbq": tbq, "tsq": tsq,
        "volume": vol,
        "prev": prev, "open": op, "high": hi, "low": lo,
        "symbol": sym, "token": token
    })

    # ---------- LADDER ----------
    eng = ENGINE_HUB.get(uid)
    if eng:
        try:
            await eng.ingest_tick(token, ltp, op)
        except Exception:
            lad_log.exception("Ladder ingest failed")

    # ---------- DASHBOARD BROADCAST ----------
    if sym:
        # Fast path: use RAM cache only (no Redis await in hot path)
        c = CIRCUIT_RAM.get(sym) or {}
        if not c:
            asyncio.create_task(_fetch_circuit_for_symbol(uid, sym), name=f"circuit_fetch:{uid}:{sym}")
        upper = c.get("upper")
        lower = c.get("lower")
        payload = {
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
        }
        dq = _get_queue(DASH_QUEUES, uid)
        try:
            dq.put_nowait(payload)
        except asyncio.QueueFull:
            pass


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
            _ensure_workers(uid)

            # ------------------------------------------------
            # 1️⃣ HANDLE TICKS
            # ------------------------------------------------
            if msg_type == "ticks":
                ticks = msg.get("ticks") or []
                if not ticks:
                    continue
                
                ws_log.debug("📦 Received %d ticks user=%s", len(ticks), uid)
                tq = _get_queue(TICK_QUEUES, uid)
                for t in ticks:
                    try:
                        tq.put_nowait(t)
                    except asyncio.QueueFull:
                        pass

            # ------------------------------------------------
            # 2️⃣ HANDLE ORDER UPDATE
            # ------------------------------------------------
            elif msg_type == "order_update":
                # ✅ Handle ORDER UPDATES (Rejections, Fills)
                payload = msg.get("payload")
                if payload:
                    oq = _get_queue(ORDER_QUEUES, uid, maxsize=1000)
                    try:
                        oq.put_nowait(payload)
                    except asyncio.QueueFull:
                        pass
    except Exception:
        ws_log.exception("❌ INGEST WS crashed user=%s", uid)

    finally:
        pass


# ------------------------------------------------
# Ladder Engine Hub (per-user)
# ------------------------------------------------
ENGINE_HUB: Dict[int, LadderEngine] = {}
POSITIONS_COUNT_CACHE: Dict[int, Dict[str, Any]] = {}

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

async def _get_live_positions_count(user_id: int, fallback: Optional[int] = None, ttl_sec: int = 5) -> int:
    """
    Return Zerodha live positions count (net positions with qty != 0).
    Uses small RAM cache to avoid frequent API calls.
    """
    user_id = int(user_id)
    now = time.time()
    cached = POSITIONS_COUNT_CACHE.get(user_id) or {}
    if cached and (now - float(cached.get("ts", 0) or 0)) < ttl_sec:
        return int(cached.get("count", fallback or 0) or 0)

    kite = await get_kite_for_user(user_id)
    if not kite:
        return int(fallback or 0)
    try:
        pos = kite.positions() or {}
        net = pos.get("net") or []
        cnt = 0
        for p in net:
            qty = p.get("quantity")
            try:
                if abs(float(qty or 0)) > 0:
                    cnt += 1
            except Exception:
                continue
        POSITIONS_COUNT_CACHE[user_id] = {"count": cnt, "ts": now}
        return cnt
    except Exception:
        return int(fallback or 0)

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
CIRCUIT_TTL_SEC = 24 * 60 * 60                # keep for a full trading day
_CIRCUIT_FILE = "circuit_cache.json"
OPEN_RAM: Dict[str, Dict[str, Any]] = {}      # symbol -> {"open":..,"ts":..}
_OPEN_FETCH_INFLIGHT: Dict[int, set] = defaultdict(set)
OPEN_TTL_SEC = 12 * 60 * 60                   # cache open price for trading day window
_CIRCUIT_FETCH_INFLIGHT: Dict[int, set] = defaultdict(set)
PREV_RAM: Dict[str, Dict[str, Any]] = {}      # symbol -> {"close":..,"ts":..}
PREV_TTL_SEC = 36 * 60 * 60                   # keep previous close for a day+ buffer

def _now_ist():
    ist = pytz.timezone("Asia/Kolkata")
    return datetime.datetime.now(ist)

def _post_squareoff_halt_key(user_id: int) -> str:
    day = _now_ist().strftime("%Y%m%d")
    return f"halt:post_squareoff:{int(user_id)}:{day}"

def _daily_trade_key(user_id: int) -> str:
    day = _now_ist().strftime("%Y%m%d")
    return f"ladder:daily_total:{int(user_id)}:{day}"

def _seconds_until_midnight_ist() -> int:
    now = _now_ist()
    tomorrow = (now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(60, int((tomorrow - now).total_seconds()))

async def _get_daily_trade_cap(user_id: int, fallback: int = 0) -> int:
    try:
        raw = await r.get(f"universal:settings:{int(user_id)}")
        if raw:
            s = json.loads(raw) or {}
            cap = int(s.get("max_trades_per_day") or 0)
            if cap > 0:
                return cap
            # fallback to existing field if not set
            cap2 = int(s.get("max_trades_per_symbol") or 0)
            if cap2 > 0:
                return cap2
    except Exception:
        return fallback
    return fallback

async def _daily_trade_count(user_id: int) -> int:
    try:
        raw = await r.get(_daily_trade_key(user_id))
        if raw not in (None, ""):
            return int(raw or 0)
    except Exception:
        return 0
    # Fallback: derive count from today's ladder states
    try:
        today = _now_ist().strftime("%Y-%m-%d")
        keys = await r.keys(f"ladder:state:{int(user_id)}:*")
        total = 0
        for k in keys:
            data = await r.get(k)
            if not data:
                continue
            try:
                s = json.loads(data)
            except Exception:
                continue
            if s.get("session_day") != today:
                continue
            st = (s.get("leg_state") or {}).get("status") or ""
            st = str(st).upper()
            if st in ("REJECTED", "ERROR", "CANCELLED", "FAILURE"):
                continue
            total += 1
        if total > 0:
            await r.set(_daily_trade_key(user_id), str(total), ex=_seconds_until_midnight_ist())
        return total
    except Exception:
        return 0

async def _incr_daily_trade(user_id: int) -> int:
    try:
        key = _daily_trade_key(user_id)
        val = await r.incr(key)
        if val == 1:
            await r.expire(key, _seconds_until_midnight_ist())
        return int(val)
    except Exception:
        return 0

async def _is_ladder_completed_today(user_id: int, symbol: str) -> bool:
    try:
        key = f"ladder:state:{int(user_id)}:{symbol}"
        raw = await r.get(key)
        if not raw:
            return False
        data = json.loads(raw)
        day = data.get("session_day")
        today = _now_ist().strftime("%Y-%m-%d")
        if day and day != today:
            return False
        active = bool(data.get("active", True))
        st = (data.get("leg_state") or {}).get("status") or ""
        st = str(st).upper()
        if not active or st in ("COMPLETE", "DONE"):
            return True
    except Exception:
        return False
    return False

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
    if c:
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


def _load_circuit_file_to_ram() -> None:
    try:
        if not os.path.exists(_CIRCUIT_FILE):
            return
        with open(_CIRCUIT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        if isinstance(data, dict):
            for sym, payload in data.items():
                s = str(sym or "").upper().strip()
                if not s or not isinstance(payload, dict):
                    continue
                CIRCUIT_RAM[s] = payload
    except Exception:
        pass


async def _fetch_circuit_for_symbol(user_id: int, sym: str) -> None:
    """
    Fetch UC/LC for a single symbol on-demand when cache is missing.
    """
    sym = (sym or "").upper().strip()
    if not sym:
        return
    inflight = _CIRCUIT_FETCH_INFLIGHT[int(user_id)]
    if sym in inflight:
        return
    inflight.add(sym)
    try:
        kite = await get_kite_for_user(int(user_id))
        if not kite:
            return
        q = kite.quote([f"NSE:{sym}"])
        v = (q or {}).get(f"NSE:{sym}") or {}
        upper = v.get("upper_circuit_limit")
        lower = v.get("lower_circuit_limit")
        payload = {
            "upper": float(upper) if upper is not None else None,
            "lower": float(lower) if lower is not None else None,
            "ts": time.time()
        }
        CIRCUIT_RAM[sym] = payload
        await r.setex(f"circuit:{sym}", CIRCUIT_TTL_SEC, json.dumps(payload))
    except Exception:
        pass
    finally:
        inflight.discard(sym)


async def get_cached_open_price(user_id: int, sym: str) -> float:
    """
    Fetch and cache day's open price.
    Priority: RAM -> live ticks (no REST quotes).
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

    # 2) Live ticks fallback (no REST quotes)
    try:
        snap = TICK_HUB.get_snapshot_for_user(int(user_id), only_symbols={sym})
        t = snap.get(sym, {}) if isinstance(snap, dict) else {}
        val = float(t.get("open") or 0.0)
        if val > 0:
            OPEN_RAM[sym] = {"open": val, "ts": now_ts}
            return val
    except Exception:
        return 0.0

    return 0.0


async def _prefetch_open_price(user_id: int, sym: str) -> None:
    sym = (sym or "").upper().strip()
    if not sym:
        return
    inflight = _OPEN_FETCH_INFLIGHT[int(user_id)]
    if sym in inflight:
        return
    inflight.add(sym)
    try:
        op = await get_cached_open_price(int(user_id), sym)
        if op and op > 0:
            OPEN_RAM[sym] = {"open": float(op), "ts": time.time()}
    except Exception:
        pass
    finally:
        inflight.discard(sym)


async def get_cached_prev_close(sym: str) -> float:
    """
    Fetch and cache previous day's close.
    Priority: RAM -> Redis.
    """
    sym = (sym or "").upper().strip()
    if not sym:
        return 0.0

    now_ts = time.time()
    c = PREV_RAM.get(sym)
    if c and (now_ts - float(c.get("ts", 0) or 0)) < PREV_TTL_SEC:
        try:
            return float(c.get("close") or 0.0)
        except Exception:
            return 0.0

    day = _now_ist().strftime("%Y%m%d")
    redis_key = f"prev:{day}:{sym}"
    raw = await r.get(redis_key)
    if raw not in (None, ""):
        try:
            val = float(raw)
            if val > 0:
                PREV_RAM[sym] = {"close": val, "ts": now_ts}
                return val
        except Exception:
            pass

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
                "ladder_cycles": 1, "max_adds_per_leg": 2, "max_trades_per_symbol": 1,
                "fast_entry_no_ltp": False
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
            "max_trades_per_symbol": 1,
            "fast_entry_no_ltp": False
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
    TTL = 60 * 60 * 24   # 24 hours (persist for full day)

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


@app.post("/api/circuit/reload")
async def reload_circuit_cache(user_id: int = 0):
    """
    Reload circuit_cache.json into RAM (API + LadderEngine).
    If user_id=0, reload for all running engines.
    """
    _load_circuit_file_to_ram()
    if user_id:
        eng = ENGINE_HUB.get(int(user_id))
        if eng:
            eng.reload_circuit_file()
        return {"status": "ok", "reloaded": int(user_id)}

    for eng in ENGINE_HUB.values():
        try:
            eng.reload_circuit_file()
        except Exception:
            pass
    return {"status": "ok", "reloaded": "all"}


@app.get("/api/symbols")
async def api_symbols(q: str = "", limit: int = 30):
    q = (q or "").strip().upper()
    if not GLOBAL_SYMBOL_LIST:
        load_instruments_file()
    if not q:
        return {"symbols": GLOBAL_SYMBOL_LIST[:limit]}
    out = []
    for s in GLOBAL_SYMBOL_LIST:
        if s.startswith(q):
            out.append(s)
            if len(out) >= limit:
                break
    return {"symbols": out}



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

    recv_ts = time.time()
    ws_log.info("\033[1mChartink webhook hit\033[0m | content-type=%s",  request.headers.get("content-type")) 

    # Backward compatible default (existing behavior was fixed to user 1).
    # If sender passes user_id, route alerts to that user.
    try:
        user_id = int(data.get("user_id") or request.query_params.get("user_id") or 1)
    except Exception:
        user_id = 1
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
    ws_log.debug("Chartink payload | scan=%s | stocks_raw_type=%s", scan_name, type(stocks_raw).__name__)

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

    ws_log.debug("Chartink raw symbols | count=%s | sample=%s", len(raw_symbols), raw_symbols[:10])

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
            try:
                tkey = f"timing:{user_id}:{symbol}"
                await r.hset(tkey, mapping={"webhook_ts": recv_ts, "subscribe_ts": time.time()})
                await r.expire(tkey, 60 * 60)
            except Exception:
                pass
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
    ws_log.info(
    "\033[1m[%s] 📥📩 Chartink Webhook received :-> user=%s | scan=%s | symbols=%s\033[0m",now.strftime("%H:%M:%S"),user_id,scan_name,parsed_symbols,)

    # 🚀 Speed-first: start auto-trades immediately on webhook (no Redis wait)
    async def _start_auto_trades_immediate():
        try:
            mode = await r.get(f"automation:mode:{user_id}")
            if (mode or "MANUAL").upper() != "AUTO":
                return

            ist = pytz.timezone("Asia/Kolkata")
            now_local = datetime.datetime.now(ist)
            if now_local.weekday() >= 5:
                return
            open_dt = now_local.replace(hour=9, minute=15, second=0, microsecond=0)
            close_dt = now_local.replace(hour=15, minute=30, second=0, microsecond=0)
            if now_local < open_dt or now_local >= close_dt:
                return

            symbols = list(new_stocks)
            if not symbols:
                return

            settings_payload = {}
            raw_settings = await r.get(f"universal:settings:{user_id}")
            if raw_settings:
                try:
                    settings_payload = json.loads(raw_settings) or {}
                except Exception:
                    settings_payload = {}

            # Entry threshold check (use universal settings)
            entry_enabled = bool(settings_payload.get("entry_threshold_enabled", False))
            try:
                entry_thresh = float(settings_payload.get("entry_threshold_pct", 0.0) or 0.0)
            except Exception:
                entry_thresh = 0.0

            # Pre-filter symbols by threshold if enabled
            ltp_open_cache = {}
            side_by_sym = {}
            if entry_enabled and entry_thresh > 0:
                async def _resolve_ltp_open(sym: str, max_wait: float = 0.2, interval: float = 0.02):
                    deadline = time.time() + max_wait
                    last_ltp = 0.0
                    last_opn = 0.0
                    while True:
                        snap = TICK_HUB.get_snapshot_for_user(int(user_id), only_symbols={sym})
                        t = snap.get(sym, {}) if isinstance(snap, dict) else {}
                        ltp = float(t.get("ltp") or 0.0)
                        opn = float(t.get("open") or 0.0)
                        if opn <= 0:
                            opn = await get_cached_open_price(int(user_id), sym)
                        last_ltp, last_opn = ltp, opn
                        if opn > 0 and ltp > 0:
                            return sym, ltp, opn, True
                        if time.time() >= deadline:
                            return sym, last_ltp, last_opn, False
                        await asyncio.sleep(interval)

                eligible = []
                skip_missing = 0
                skip_notmet = 0
                missing_details = []
                notmet_details = []
                eligible_details = []
                tasks = [asyncio.create_task(_resolve_ltp_open(sym)) for sym in symbols]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    try:
                        if isinstance(res, Exception):
                            skip_missing += 1
                            if len(missing_details) < 3:
                                missing_details.append("ERR")
                            ws_log.info("THRESHOLD_CHECK result=SKIP reason=ERR")
                            continue
                        sym, ltp, opn, ok = res
                        if ok and opn > 0 and ltp > 0:
                            diff_pct = ((ltp - opn) / opn) * 100.0
                            if diff_pct >= entry_thresh:
                                eligible.append(sym)
                                ltp_open_cache[sym] = (ltp, opn)
                                side_by_sym[sym] = "BUY"
                                if len(eligible_details) < 3:
                                    eligible_details.append(f"{sym} BUY diff={diff_pct:.2f}% open={opn:.2f} ltp={ltp:.2f}")
                                ws_log.info(
                                    "THRESHOLD_CHECK symbol=%s side=BUY result=PASS diff=%.2f%% thresh=%.2f open=%.2f ltp=%.2f",
                                    sym, diff_pct, entry_thresh, opn, ltp
                                )
                            elif diff_pct <= -entry_thresh:
                                eligible.append(sym)
                                ltp_open_cache[sym] = (ltp, opn)
                                side_by_sym[sym] = "SELL"
                                if len(eligible_details) < 3:
                                    eligible_details.append(f"{sym} SELL diff={diff_pct:.2f}% open={opn:.2f} ltp={ltp:.2f}")
                                ws_log.info(
                                    "THRESHOLD_CHECK symbol=%s side=SELL result=PASS diff=%.2f%% thresh=%.2f open=%.2f ltp=%.2f",
                                    sym, diff_pct, entry_thresh, opn, ltp
                                )
                            else:
                                skip_notmet += 1
                                if len(notmet_details) < 3:
                                    notmet_details.append(f"{sym} diff={diff_pct:.2f}% open={opn:.2f} ltp={ltp:.2f}")
                                ws_log.debug("AUTO_SKIP threshold not met | user=%s scan=%s symbol=%s diff=%.2f thresh=%.2f",
                                             user_id, scan_name, sym, diff_pct, entry_thresh)
                                side_hint = "BUY" if diff_pct > 0 else "SELL"
                                ws_log.info(
                                    "THRESHOLD_CHECK symbol=%s side=%s result=SKIP reason=NOT_MET diff=%.2f%% thresh=%.2f open=%.2f ltp=%.2f",
                                    sym, side_hint, diff_pct, entry_thresh, opn, ltp
                                )
                        else:
                            skip_missing += 1
                            if len(missing_details) < 3:
                                missing_details.append(f"{sym} open={opn} ltp={ltp}")
                            ws_log.debug("AUTO_SKIP threshold data missing | user=%s scan=%s symbol=%s ltp=%s open=%s",
                                         user_id, scan_name, sym, ltp, opn)
                            ws_log.info(
                                "THRESHOLD_CHECK symbol=%s result=SKIP reason=MISSING open=%s ltp=%s thresh=%.2f",
                                sym, opn, ltp, entry_thresh
                            )
                            try:
                                scan_bucket = re.sub(r"[^A-Z0-9_-]+", "", str(scan_name or "GLOBAL").upper()) or "GLOBAL"
                                pending_key = f"auto:pending:{user_id}:{scan_bucket}"
                                await r.sadd(pending_key, sym)
                                await r.expire(pending_key, 60 * 60 * 16)
                            except Exception:
                                pass
                    except Exception:
                        skip_missing += 1
                        ws_log.debug("AUTO_SKIP threshold check failed | user=%s scan=%s symbol=%s",
                                     user_id, scan_name, sym)
                symbols = eligible
                summary_parts = [
                    f"eligible={len(eligible)}",
                    f"skip_missing={skip_missing}",
                    f"skip_notmet={skip_notmet}",
                    f"thresh={entry_thresh:.2f}",
                    "rule=diff>=thresh_BUY|diff<=-thresh_SELL",
                ]
                if eligible_details:
                    summary_parts.append("eligible_sample=" + "; ".join(eligible_details))
                if missing_details:
                    summary_parts.append("missing_sample=" + "; ".join(missing_details))
                if notmet_details:
                    summary_parts.append("notmet_sample=" + "; ".join(notmet_details))
                ws_log.info("THRESHOLD_SUMMARY " + " ".join(summary_parts))
            
            if not symbols:
                return

            eng = await get_engine_for_user(int(user_id))
            if not eng:
                return
            # Daily total trades cap (strict)
            try:
                daily_cap = await _get_daily_trade_cap(int(user_id), 0)
                if daily_cap > 0:
                    cur_total = await _daily_trade_count(int(user_id))
                    remaining_daily = max(0, daily_cap - cur_total)
                    if remaining_daily <= 0:
                        ws_log.info("AUTO_DAILY_CAP_REACHED user=%s cap=%s total=%s",
                                    user_id, daily_cap, cur_total)
                        return
                    if len(symbols) > remaining_daily:
                        symbols = symbols[:remaining_daily]
            except Exception:
                pass
            # Active positions limit check before dispatch
            scan_bucket = re.sub(r"[^A-Z0-9_-]+", "", str(scan_name or "GLOBAL").upper()) or "GLOBAL"
            try:
                max_limit = int(settings_payload.get("max_trades_per_symbol") or 0)
            except Exception:
                max_limit = 0
            if max_limit > 0:
                current_active = await _get_live_positions_count(int(user_id), fallback=_count_active_sessions(eng))
                remaining = max(0, max_limit - current_active)
                ws_log.info("AUTO_LIMIT_CHECK | user=%s scan=%s active=%s limit=%s remaining=%s",
                            user_id, scan_bucket, current_active, max_limit, remaining)
                if current_active >= max_limit:
                    ws_log.info("AUTO_SKIP limit reached | user=%s scan=%s active=%s limit=%s",
                                user_id, scan_bucket, current_active, max_limit)
                    return
                if remaining <= 0:
                    return
                symbols = symbols[:remaining]

            batch_size = 10
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                tasks = []
                for sym in batch:
                    ws_log.info(
                        "AUTO_DISPATCH user=%s symbol=%s alert_id=%s alert_time=%s recv_ts=%.3f dispatch_ts=%.3f delta_ms=%d",
                        user_id,
                        sym,
                        packet.get("id"),
                        packet.get("time"),
                        recv_ts,
                        time.time(),
                        int((time.time() - recv_ts) * 1000)
                    )
                    ltp_hint, open_hint = ltp_open_cache.get(sym, (None, None))
                    side_hint = side_by_sym.get(sym, "BUY")
                    tasks.append(eng.start_ladder(
                        symbol=sym,
                        started_side=side_hint,
                        settings_mode="UNIVERSAL",
                        settings_payload=settings_payload,
                        scan_name=scan_name,
                        ltp_hint=ltp_hint,
                        open_hint=open_hint,
                        alert_ts=recv_ts,
                        dispatch_ts=time.time(),
                    ))
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    # Increment daily trade count for successful starts
                    try:
                        for res in results:
                            if isinstance(res, dict) and res.get("error"):
                                continue
                            if isinstance(res, dict) and res.get("status") in ("waiting_for_feed", "waiting_for_ltp", "started"):
                                await _incr_daily_trade(int(user_id))
                    except Exception:
                        pass
        except Exception:
            ws_log.exception("Webhook auto-trade immediate failed | user=%s", user_id)

    # ✅ REAL-TIME PUSH to dashboard (WS) first (so UI updates before dispatch)
    try:
        await TICK_HUB.broadcast(user_id, {"type": "alert", "alert": packet})
    except Exception:
        ws_log.exception("Chartink WS push failed | user=%s", user_id)

    # Dispatch auto-trades right after WS push (no Redis wait)
    asyncio.create_task(_start_auto_trades_immediate())
    
    # 🔴 Await Redis calls (store after dispatch)
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
    count = len(raw)
    if count > 0:
        head = raw[0] if raw else None
        sig = (redis_key, head)
        if LAST_ALERTS_LOG["key"] != sig:
            ws_log.info("GET /api/alerts | user=%s | key=%s | count=%s", user_id, redis_key, count)
            LAST_ALERTS_LOG["key"] = sig

    # Backward compatibility: webhook may have been writing to user 1.
    if not raw and int(user_id) != 1:
        fallback_key = f"chartink_alerts:1:{today}"
        raw = await r.lrange(fallback_key, 0, -1)
        ws_log.info("GET /api/alerts fallback | key=%s | count=%s", fallback_key, len(raw))

    alerts = [json.loads(x) for x in raw]

    # ✅ Ensure WS tokens for symbols in alerts so dashboard fills values after refresh
    try:
        uid = int(user_id)
        eng = ENGINE_HUB.get(uid)
        for pkt in alerts:
            for s in (pkt.get("stocks") or []):
                sym = str(s or "").strip().upper()
                if not sym:
                    continue
                tok = SYMBOL_TOKEN_RAM.get(uid, {}).get(sym) or GLOBAL_SYMBOL_TOKEN.get(sym)
                if tok:
                    SYMBOL_TOKEN_RAM[uid][sym] = int(tok)
                    add_ws_token(uid, int(tok))
                    if eng:
                        if getattr(eng, "SYMBOL_TOKEN_RAM", None) is None:
                            eng.SYMBOL_TOKEN_RAM = {}
                        eng.SYMBOL_TOKEN_RAM.setdefault(uid, {})[sym] = int(tok)
    except Exception:
        ws_log.exception("Alerts token ensure failed | user=%s", user_id)

    return alerts


@app.post("/api/alerts/add")
async def add_alert_symbol(request: Request):
    """
    Add a symbol to today's alerts list so it appears in the table immediately.
    """
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    try:
        data = await request.json()
    except Exception:
        data = {}
    symbol = str(data.get("symbol") or "").strip().upper()
    scan = str(data.get("scan") or "MANUAL").strip()
    if not symbol:
        return JSONResponse({"error": "Symbol missing"}, status_code=400)

    # Ensure WS token if available
    tok = SYMBOL_TOKEN_RAM.get(int(user_id), {}).get(symbol) or GLOBAL_SYMBOL_TOKEN.get(symbol)
    if tok:
        add_ws_token(int(user_id), int(tok))

    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.datetime.now(ist).strftime("%Y-%m-%d")
    redis_key = f"chartink_alerts:{user_id}:{today}"
    packet = {
        "id": int(time.time() * 1000),
        "scan": scan,
        "stocks": [symbol],
        "time": datetime.datetime.now(ist).strftime("%H:%M:%S"),
    }
    await r.lpush(redis_key, json.dumps(packet))
    await r.ltrim(redis_key, 0, 200)
    return {"status": "added", "symbol": symbol}

@app.post("/api/instruments/refresh")
async def refresh_instruments(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    try:
        asyncio.create_task(_run_fetch_circuit_job(int(user_id)))
        return {"status": "started"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/zerodha/refresh")
async def refresh_zerodha(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    try:
        uid = int(user_id)
        eng = ENGINE_HUB.get(uid)
        if eng:
            await eng.stop()
        ENGINE_HUB.pop(uid, None)
        eng = await get_engine_for_user(uid)
        if not eng:
            return JSONResponse({"error": "No valid api_key/access_token"}, status_code=400)
        # Ask worker to rebuild WS client immediately
        try:
            await r.setex(f"ws:refresh:{uid}", 60, "1")
        except Exception:
            pass
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)



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
        # Block all new ladders after auto square-off for the day
        try:
            if await r.exists(_post_squareoff_halt_key(int(user_id))):
                return JSONResponse({"error": "AUTO_SQUAREOFF_DONE: trades blocked for today"}, 403)
        except Exception:
            pass

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
            scan_name="MANUAL",
        )
        # if engine is waiting for feed, return clean response
        if isinstance(resp, dict) and resp.get("status") in ("waiting_for_feed", "waiting_for_ltp"):
            return {
                "status": resp["status"],
                "symbol": symbol,
                "message": resp.get("message", "Waiting for feed..."),
                "session": resp.get("session"),
            }

        if isinstance(resp, dict) and resp.get("error"):
            return resp

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

        # Attach auto-retry limit status (only when remaining=0)
        limit_state = AUTO_RETRY_LIMIT_STATE.get(int(user_id)) or {}
        if limit_state and int(limit_state.get("remaining", 1)) == 0:
            for s in filtered:
                if isinstance(s, dict):
                    s["limit_block"] = True
                    s["limit_scan"] = limit_state.get("scan")
                    s["limit_active"] = limit_state.get("active")
                    s["limit_max"] = limit_state.get("limit")
                    s["limit_remaining"] = limit_state.get("remaining")

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
        # Prevent concurrent/double squareoff for same user
        lock_key = f"squareoff_all:lock:{user_id}"
        try:
            locked = await r.set(lock_key, "1", nx=True, ex=60)
            if not locked:
                return {"error": "Squareoff already in progress"}
        except Exception:
            pass
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

        # 1) Place squareoff orders for all open positions (dedupe by symbol)
        pos_by_sym: Dict[str, int] = {}
        for p in pos:
            sym = str(p.get("tradingsymbol", "")).upper()
            q = int(p.get("quantity", 0) or 0)
            if not sym or q == 0:
                continue
            pos_by_sym[sym] = pos_by_sym.get(sym, 0) + q

        for sym, q in pos_by_sym.items():
            if q == 0:
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

        # 4) Optional: remove tokens from WS RAM set (worker reads /api/ws-tokens)
        # If you want this, only remove tokens for symbols cleanup.
        for sym in symbols_to_cleanup:
            token = _resolve_token(user_id, sym)
            if token:
                remove_ws_token(user_id, int(token))

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
    finally:
        try:
            await r.delete(lock_key)
        except Exception:
            pass


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

        # ✅ Trigger window: 15:20–15:29 IST (catch-up if server was busy)
        if now.hour == 15 and 20 <= now.minute < 30 and triggered_today != today_date:
            # Hard guard: run only once per day (even if loop re-enters)
            try:
                guard_key = f"auto_squareoff:done:{now.strftime('%Y%m%d')}"
                ok = await r.set(guard_key, "1", nx=True, ex=60 * 60 * 8)
                if not ok:
                    await asyncio.sleep(1)
                    continue
            except Exception:
                pass
            logging.info("=" * 60)
            logging.info("⏰ AUTO SQUARE-OFF TRIGGERED window (15:20-15:29 IST)")
            logging.info("=" * 60)

            triggered_today = today_date  # Mark as triggered for today

            # Build user list from access_token:* and fallback to zerodha:keys:*
            user_ids: set[int] = set()
            cursor = 0
            while True:
                cursor, keys = await r.scan(cursor=cursor, match="access_token:*", count=200)
                for key in keys:
                    try:
                        uid_str = key.decode().split(":")[-1] if isinstance(key, bytes) else key.split(":")[-1]
                        if uid_str.isdigit():
                            user_ids.add(int(uid_str))
                    except Exception:
                        pass
                if cursor == 0:
                    break

            if not user_ids:
                cursor = 0
                while True:
                    cursor, keys = await r.scan(cursor=cursor, match="zerodha:keys:*", count=200)
                    for key in keys:
                        try:
                            uid_str = key.decode().split(":")[-1] if isinstance(key, bytes) else key.split(":")[-1]
                            if uid_str.isdigit():
                                user_ids.add(int(uid_str))
                        except Exception:
                            pass
                    if cursor == 0:
                        break

            keys_found = 0
            for user_id in user_ids:
                try:
                    if not await r.exists(f"access_token:{user_id}"):
                        continue
                    # Immediately block auto-retry and clear pending queues for this user
                    try:
                        await r.set(_post_squareoff_halt_key(user_id), "1", ex=60 * 60 * 12)
                        logging.info("AUTO_HALTED user=%s reason=auto_squareoff", user_id)
                    except Exception:
                        pass
                    try:
                        cursor2 = 0
                        while True:
                            cursor2, pkeys = await r.scan(cursor=cursor2, match=f"auto:pending:{user_id}:*", count=200)
                            if pkeys:
                                await r.delete(*pkeys)
                            if cursor2 == 0:
                                break
                        logging.info("AUTO_PENDING_CLEARED user=%s", user_id)
                    except Exception:
                        pass
                    keys_found += 1
                    # ✅ FIXED: Call extracted function, NOT the route handler
                    sq_res = await execute_squareoff_all(user_id)
                    if "error" not in sq_res:
                        # Force automation to MANUAL after auto square-off
                        try:
                            await r.set(f"automation:mode:{user_id}", "MANUAL")
                        except Exception:
                            pass
                        evt_key = f"auto_squareoff:event:{user_id}"
                        evt_payload = {
                            "ts": int(time.time()),
                            "time_ist": now.strftime("%H:%M:%S"),
                            "count": int(sq_res.get("count", 0)),
                            "status": "SUCCESS",
                        }
                        await r.set(evt_key, json.dumps(evt_payload), ex=60 * 60 * 12)
                except Exception as e:
                    logging.error(f"Error processing auto square-off for user {user_id}: {e}")
            if keys_found == 0:
                logging.warning("AUTO SQUARE-OFF: No access_token keys found in Redis")
            logging.info("=" * 60)
            if keys_found == 0:
                logging.info("✅ [ AUTO SQUARE-OFF ] COMPLETED | USERS=0 | STATUS=NO_TOKENS")
            else:
                logging.info("✅ [ AUTO SQUARE-OFF ] COMPLETED | USERS=%s | STATUS=SUCCESS", keys_found)
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
        "automation:mode:*",
        "universal:settings:*",
        "stock:settings:*",
        "ladder:settings:universal:*",
        "ladder:settings:stock:*",
        "ladder:state:*",
        "ladder:lock:*",
        "ladder:events:*",
        "ladder:count:*",
        "ladder:global_count:*",
        "halt:insuff:*",
        "chartink_alerts:*",
    ]

    while True:
        now = _now_ist()
        today_date = now.date()

        if now.hour == 8 and now.minute == 0 and triggered_today != today_date:
            logging.warning("🧹🧹 DAILY RESET TRIGGERED at 08:00 IST")
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
        # Block all new trades after auto square-off for the day
        try:
            if await r.exists(_post_squareoff_halt_key(int(user_id))):
                return JSONResponse({"error": "AUTO_SQUAREOFF_DONE: trades blocked for today"}, 403)
        except Exception:
            pass

        kite = await get_kite_for_user(int(user_id))
        if not kite:
             return JSONResponse({"error": "Kite not connected"}, 401)

        # ✅ Price band check (user request): allow only 100–4000 LTP
        try:
            snap = TICK_HUB.get_snapshot_for_user(int(user_id), only_symbols={symbol})
            ltp = float((snap.get(symbol, {}) or {}).get("ltp") or 0)
        except Exception:
            ltp = 0

        if ltp <= 0:
            return JSONResponse({"error": "LTP not available from live ticks"}, 400)
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

    # 2) remove token from WS RAM set (worker reads /api/ws-tokens)
    token = _resolve_token(int(user_id), symbol)
    if token:
        remove_ws_token(int(user_id), int(token))

    # 3) also remove in-memory ladder session (IMPORTANT)
    eng = ENGINE_HUB.get(int(user_id))
    if eng:
        try:
            # we'll implement this method in ladder_engine.py next:
            await eng.remove_session(symbol)
        except Exception:
            pass

    return {"status": "deleted", "symbol": symbol}
@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)
