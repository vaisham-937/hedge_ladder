
"""
ladder_engine.py

Ultra-low latency Ladder Trading Engine (BUY ladder + SELL ladder) for Zerodha Kite.

Design goals:
- Hot path (ticks + ladder state machine) runs in-memory.
- Redis is used only for control/state snapshot, settings, locks (Lua), and kill-switch.
- No double entry / double exit via Redis Lua atomic lock.
- All orders are MIS market orders.

This engine is designed to be launched from main.py (FastAPI) and run per-user.

✅ What this version adds (your request):
- FULL detailed logs visible in terminal (INFO/WARN/ERROR, plus optional DEBUG)
- Redis event log list: ladder:events:{user_id} (last 200 events)
- Logs for: START, ENTRY, ADD, EXIT, LOCK BUSY, KILL, CIRCUIT, SNAPSHOT failures

How to see logs in terminal:
- If LadderEngine runs inside FastAPI: run uvicorn with log-level debug/info
    uvicorn main:app --host 127.0.0.1 --port 8000 --log-level debug
- If LadderEngine is started from main.py, its logs will print in the same terminal.
"""

from __future__ import annotations

import asyncio
import json
import re
import logging
import math
import os
import sys
import time, datetime
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Any, Dict, Optional, Literal, List, Tuple
from collections import defaultdict
from kiteconnect import KiteConnect

try:
    import redis.asyncio as aioredis  # redis-py >= 4.2
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore


start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ==========================================================
# Logging (Terminal-friendly)
# ==========================================================
logger = logging.getLogger("ladder_engine")


def _env_log_level() -> int:
    lvl = (os.getenv("LADDER_LOG_LEVEL") or "").strip().upper()
    if lvl == "DEBUG":
        return logging.DEBUG
    if lvl == "WARNING" or lvl == "WARN":
        return logging.WARNING
    if lvl == "ERROR":
        return logging.ERROR
    return logging.INFO


def _color_only_enabled() -> bool:
    return (os.getenv("LADDER_LOG_COLOR_ONLY") or "").strip() in ("1", "true", "TRUE", "yes", "YES")


class _ColorOnlyFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Always allow errors
        if record.levelno >= logging.ERROR:
            return True
        try:
            msg = record.getMessage()
        except Exception:
            return False
        # Keep only ANSI-colored lines (escape sequence)
        return "\x1b[" in msg


def setup_logger() -> None:
    level = _env_log_level()
    logger.setLevel(level)

    # Avoid duplicate handlers (especially with reload)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        h = logging.StreamHandler(sys.stdout)
        h.setLevel(level)
        fmt = logging.Formatter("%(asctime)s %(message)s")
        h.setFormatter(fmt)
        if _color_only_enabled():
            h.addFilter(_ColorOnlyFilter())
        logger.addHandler(h)

    # Keep clean (don’t spam root logger)
    logger.propagate = False
setup_logger()


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _color_side_word(side: str) -> str:
    s = (side or "").upper()
    if s == "BUY":
        return "\033[92mBUY\033[0m"
    if s == "SELL":
        return "\033[91mSELL\033[0m"
    return s


async def push_event(redis, user_id: int, symbol: str, event: str, data: Optional[Dict[str, Any]] = None) -> None:
    """
    Stores last 200 ladder events in Redis list: ladder:events:{user_id}
    This helps you debug even when dashboard says "No ladders running".
    """
    try:
        key = f"ladder:events:{user_id}"
        payload = {
            "ts": _ts(),
            "symbol": symbol,
            "event": event,
            "data": data or {},
        }
        await redis.lpush(key, json.dumps(payload))
        await redis.ltrim(key, 0, 200)
    except Exception:
        # Never crash hot path due to logging
        return


Side = Literal["BUY", "SELL"]
QtyMode = Literal["CAPITAL", "QTY"]
SettingsMode = Literal["UNIVERSAL", "STOCK"]

def _supports_color() -> bool:
    return False


def _c(text: str, code: str = "0") -> str:
    return str(text)


# ==========================================================
# Redis keys
# ==========================================================
def k_kill(user_id: int) -> str:
    return f"kill:{user_id}"

def k_settings_universal(user_id: int) -> str:
    return f"ladder:settings:universal:{user_id}"

def k_settings_stock(user_id: int, symbol: str) -> str:
    return f"ladder:settings:stock:{user_id}:{symbol}"

def k_state(user_id: int, symbol: str) -> str:
    return f"ladder:state:{user_id}:{symbol}"

def k_lock(user_id: int, symbol: str, action: str) -> str:
    # action: entry/exit/add
    return f"ladder:lock:{user_id}:{symbol}:{action}"

def k_circuit(symbol: str) -> str:
    return f"circuit:{symbol}"

# ==========================================================
# Redis Lua: lock with kill check
# ==========================================================
LUA_LOCK = r"""
-- KEYS[1] = lock_key
-- KEYS[2] = kill_key
-- ARGV[1] = ttl_ms
-- ARGV[2] = now_ms
if redis.call('EXISTS', KEYS[2]) == 1 then
  return -2
end
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('PSETEX', KEYS[1], ARGV[1], ARGV[2])
return 1
"""

# ==========================================================
# Data models
# ==========================================================
@dataclass
class LadderSettings:
    qty_mode: QtyMode = "CAPITAL"         # CAPITAL or QTY
    per_trade_capital: float = 1000.0      # used when qty_mode == CAPITAL
    per_trade_qty: int = 1                # used when qty_mode == QTY
    threshold_pct: float = 1.5       # add threshold per step (%)
    stop_loss_pct: float = 1.5            # immediate SL from avg (%)
    trailing_sl_pct: float = 1.5         # trailing from peak/low (%)
    ladder_cycles: int = 3               # 1 cycle = 1 buy ladder + 1 sell ladder
    max_adds_per_leg: int = 4           # safety cap
    slippage_bps: float = 0.0             # optional, not used for MIS market in this version
    max_trades_per_symbol: int = 1        # Max number of auto-trades allowed per symbol per session
    entry_threshold_enabled: bool = False # ✅ NEW: Entry Logic
    entry_threshold_pct: float = 1.5      # ✅ NEW: Entry Logic %
    fast_entry_no_ltp: bool = False       # ✅ Speed-first: allow entry without live LTP (uses per_trade_qty)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "LadderSettings":
        s = LadderSettings()
        for f in asdict(s).keys():
            if f in d and d[f] is not None:
                setattr(s, f, d[f])
        # sanitize
        s.per_trade_qty = int(max(1, int(s.per_trade_qty)))
        s.per_trade_capital = float(max(0.0, float(s.per_trade_capital)))
        s.threshold_pct = float(max(0.01, float(s.threshold_pct)))
        s.stop_loss_pct = float(max(0.01, float(s.stop_loss_pct)))
        s.trailing_sl_pct = float(max(0.01, float(s.trailing_sl_pct)))
        s.ladder_cycles = int(max(1, int(s.ladder_cycles)))
        s.max_adds_per_leg = int(max(1, int(s.max_adds_per_leg)))
        s.max_trades_per_symbol = int(max(1, int(s.max_trades_per_symbol)))
        s.entry_threshold_enabled = bool(s.entry_threshold_enabled)
        s.entry_threshold_pct = float(s.entry_threshold_pct)
        s.fast_entry_no_ltp = bool(s.fast_entry_no_ltp)
        return s

@dataclass
class Circuit:
    upper: Optional[float] = None
    lower: Optional[float] = None
    ts: float = 0.0  # unix seconds

@dataclass
class LegState:
    side: Side
    status: Literal["IDLE", "RUNNING", "EXITING", "DONE", "ERROR"] = "IDLE"
    qty: int = 0
    avg_price: float = 0.0
    entries: int = 0
    last_add_price: float = 0.0
    highest: float = 0.0  # for BUY
    lowest: float = 0.0   # for SELL
    unrealized_pnl: float = 0.0
    reason: str = ""
    entry_price_internal: float = 0.0
    entry_price_exec: float = 0.0
    exit_price_exec: float = 0.0
    entry_order_id: str = ""
    exit_order_id: str = ""
    entry_filled: bool = False
    pending_qty: int = 0
    entry_hist_recorded: bool = False
    last_add_order_id: str = ""

@dataclass
class LadderSession:
    user_id: int
    symbol: str
    token: int
    started_side: Side
    settings_mode: SettingsMode
    settings: LadderSettings

    # cycle tracking
    cycle_index: int = 0
    leg: Side = "BUY"  # current leg side
    leg_state: LegState = None  # type: ignore

    active: bool = True
    created_ts: float = 0.0
    updated_ts: float = 0.0
    session_day: str = ""
    cycle_hist: Dict[str, Any] = field(default_factory=dict)
    alert_ts: float = 0.0
    stop_after_exit: bool = False
    pending_entry_threshold: bool = False

    def to_public_dict(self) -> Dict[str, Any]:
        d = {
            "user_id": self.user_id,
            "symbol": self.symbol,
            "token": self.token,
            "started_side": self.started_side,
            "settings_mode": self.settings_mode,
            "cycle_index": self.cycle_index,
            "cycle_total": self.settings.ladder_cycles,
            "leg": self.leg,
            "active": self.active,
            "created_ts": self.created_ts,
            "updated_ts": self.updated_ts,
            "session_day": self.session_day,
            "ltp": None,  
            "leg_state": asdict(self.leg_state) if self.leg_state else None,
            "settings": asdict(self.settings),
            "cycle_hist": self.cycle_hist,
        }
        return d


# ==========================================================
# Order worker (single-thread semantics via queue)
# ==========================================================
class OrderWorker:
    def __init__(self) -> None:
        self._q: "asyncio.Queue[Tuple[asyncio.Future, Dict[str, Any]]]" = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._task:
            return
        self._task = asyncio.create_task(self._run(), name="ladder_order_worker")
        logger.info("ORDER_WORKER started")

    async def submit(self, fn, **kwargs) -> Any:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        await self._q.put((fut, {"fn": fn, "kwargs": kwargs}))
        return await fut

    async def _run(self) -> None:
        while True:
            fut, job = await self._q.get()
            try:
                res = await asyncio.to_thread(job["fn"], **job["kwargs"])
                if not fut.cancelled():
                    fut.set_result(res)
            except Exception as e:
                if not fut.cancelled():
                    fut.set_exception(e)


# ==========================================================
# Ladder Engine (per user)
# ==========================================================
class LadderEngine:
    def __init__(self, redis_url: str, user_id: int, api_key: str, access_token: str) -> None:
        if aioredis is None:
            raise RuntimeError("redis.asyncio is not available. Please install/upgrade redis-py.")

        self.user_id = int(user_id)
        self.redis_url = redis_url
        self.redis = aioredis.from_url(redis_url, decode_responses=True)

        self.api_key = api_key
        self.access_token = access_token

        self._poll_task: Optional[asyncio.Task] = None

        self._running = False
        self._lua_lock_sha: Optional[str] = None

        # Hot path memory:
        self.ticks: Dict[int, float] = {}             # token -> ltp
        self.opens: Dict[str, float] = {}            # symbol -> open
        self.sessions: Dict[str, LadderSession] = {}  # symbol -> session
        self.circuits: Dict[str, Circuit] = {}        # symbol -> Circuit

        self.SYMBOL_TOKEN_RAM = None
        self.order_worker = OrderWorker()
        self._kill_handled = False
        self._sym_locks: Dict[str, asyncio.Lock] = {}
        self._global_limit_warned = False
        self._token_sessions: Dict[int, set] = defaultdict(set)
        self._waiting_symbols: set = set()

    def _today_key(self) -> str:
        return datetime.now().strftime("%Y%m%d")

    def _insuff_halt_key(self) -> str:
        return f"halt:insuff:{self.user_id}:{self._today_key()}"

    async def _set_insuff_halt(self, reason: str) -> None:
        try:
            await self.redis.set(self._insuff_halt_key(), reason, ex=60 * 60 * 24)
        except Exception:
            pass

    def _is_insufficient(self, msg: str) -> bool:
        m = (msg or "").lower()
        return ("insufficient" in m and "fund" in m) or ("insufficient" in m and "balance" in m) or ("insufficient capital" in m)

    async def _get_open_price(self, symbol: str) -> float:
        """
        Open price is sourced from live ticks only (no Redis dependency).
        """
        try:
            val = float(self.opens.get(symbol, 0.0) or 0.0)
            return val if val > 0 else 0.0
        except Exception:
            return 0.0

    def _sym_lock(self, symbol: str) -> asyncio.Lock:
        symbol = (symbol or "").strip().upper()
        lock = self._sym_locks.get(symbol)
        if lock is None:
            lock = asyncio.Lock()
            self._sym_locks[symbol] = lock
        return lock

    def _register_session_token(self, sess: "LadderSession") -> None:
        try:
            if sess and sess.token and sess.token > 0:
                self._token_sessions[int(sess.token)].add(sess.symbol)
        except Exception:
            pass

    def _unregister_symbol(self, symbol: str) -> None:
        symbol = (symbol or "").strip().upper()
        self._waiting_symbols.discard(symbol)
        for tok, syms in list(self._token_sessions.items()):
            if symbol in syms:
                syms.discard(symbol)
            if not syms:
                self._token_sessions.pop(tok, None)


    def log_state(self, sess, msg):
        logger.info(
            "STATE user=%s symbol=%s leg=%s qty=%s avg=%s ltp=%s | %s",
            self.user_id,
            sess.symbol,
            sess.leg_state.side if sess.leg_state else None,
            sess.leg_state.qty if sess.leg_state else 0,
            sess.leg_state.avg_price if sess.leg_state else 0,
            self.ticks.get(sess.token),
            msg
        )

    def set_symbol_token(self, symbol: str, token: int) -> None:
        symbol = symbol.strip().upper()
        if self.SYMBOL_TOKEN_RAM is None:
            self.SYMBOL_TOKEN_RAM = {}
        self.SYMBOL_TOKEN_RAM.setdefault(self.user_id, {})[symbol] = int(token)

    # ------------ lifecycle ------------
    async def start(self) -> None:
        if self._running:
            logger.warning("ENGINE already running user_id=%s", self.user_id)
            return

        self._running = True
        logger.info("BOOT user_id=%s redis_url=%s", self.user_id, self.redis_url)

        self._lua_lock_sha = await self.redis.script_load(LUA_LOCK)
        logger.info("LUA loaded sha=%s", self._lua_lock_sha)

        await self.order_worker.start()

        self._poll_task = asyncio.create_task(self._poller(), name=f"ladder_poller:{self.user_id}")

        logger.info("✅✅ LadderEngine started for user_id=%s", self.user_id)
        await push_event(self.redis, self.user_id, "-", "ENGINE_STARTED", {"user_id": self.user_id})


    async def stop(self) -> None:
        logger.warning("ENGINE stop requested user_id=%s", self.user_id)
        self._running = False

        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        await push_event(self.redis, self.user_id, "-", "ENGINE_STOPPED", {"user_id": self.user_id})
        try:
            await self.redis.close()
        except Exception:
            pass


    # ------------ redis helpers ------------
    async def _is_kill(self) -> bool:
        v = await self.redis.get(k_kill(self.user_id))
        return bool(v)

    async def set_kill(self, enabled: bool) -> None:
        if enabled:
            await self.redis.setex(k_kill(self.user_id), 60 * 60 * 24, "1")
            logger.error("KILL_SWITCH enabled user_id=%s", self.user_id)
            await push_event(self.redis, self.user_id, "-", "KILL_SWITCH_ON", {})
        else:
            await self.redis.delete(k_kill(self.user_id))
            logger.warning("KILL_SWITCH cleared user_id=%s", self.user_id)
            await push_event(self.redis, self.user_id, "-", "KILL_SWITCH_OFF", {})

    async def _lock(self, symbol: str, action: str, ttl_ms: int = 900) -> int:
        """
        Return: 1 acquired, 0 busy, -2 killed
        """
        assert self._lua_lock_sha is not None
        now_ms = int(time.time() * 1000)
        key1 = k_lock(self.user_id, symbol, action)
        key2 = k_kill(self.user_id)

        res = int(await self.redis.evalsha(self._lua_lock_sha, 2, key1, key2, str(ttl_ms), str(now_ms)))
        if res == 0:
            logger.warning("LOCK_BUSY user=%s symbol=%s action=%s", self.user_id, symbol, action)
        elif res == -2:
            logger.error("LOCK_REFUSED_KILL user=%s symbol=%s action=%s", self.user_id, symbol, action)
        else:
            logger.debug("LOCK_OK user=%s symbol=%s action=%s", self.user_id, symbol, action)
        return res

    # ------------ settings ------------
    async def load_settings(self, mode: SettingsMode, symbol: str) -> LadderSettings:
        if mode == "STOCK":
            raw = await self.redis.get(k_settings_stock(self.user_id, symbol))
            if raw:
                try:
                    return LadderSettings.from_dict(json.loads(raw))
                except Exception:
                    logger.error("SETTINGS parse failed stock user=%s symbol=%s raw=%s", self.user_id, symbol, raw)
        raw = await self.redis.get(k_settings_universal(self.user_id))
        if raw:
            try:
                return LadderSettings.from_dict(json.loads(raw))
            except Exception:
                logger.error("SETTINGS parse failed universal user=%s raw=%s", self.user_id, raw)
        return LadderSettings()

    async def save_settings(self, mode: SettingsMode, symbol: str, settings: LadderSettings) -> None:
        payload = json.dumps(asdict(settings))
        if mode == "STOCK":
            await self.redis.set(k_settings_stock(self.user_id, symbol), payload)
            logger.debug("SETTINGS saved STOCK user=%s symbol=%s settings=%s", self.user_id, symbol, payload)
        else:
            await self.redis.set(k_settings_universal(self.user_id), payload)
            logger.debug("SETTINGS saved UNIVERSAL user=%s settings=%s", self.user_id, payload)

        await push_event(self.redis, self.user_id, symbol, "SETTINGS_SAVED", {"mode": mode, "settings": asdict(settings)})

    # ------------ circuit cache ------------
    async def _fetch_quote_api(self, symbol: str) -> Dict[str, Any]:
        """Quote REST calls are disabled; return empty payload."""
        return {}

    # ------------ circuit cache ------------
    async def get_circuit(self, symbol: str, force: bool = False) -> Circuit:
        c = self.circuits.get(symbol)
        if c and not force and (time.time() - c.ts) < 300:
            return c
        raw = await self.redis.get(k_circuit(symbol))
        if raw and not force:
            try:
                d = json.loads(raw)
                c = Circuit(upper=d.get("upper"), lower=d.get("lower"), ts=float(d.get("ts", 0)))
                if (time.time() - c.ts) < 900:
                    self.circuits[symbol] = c
                    logger.debug("CIRCUIT cache(redis) symbol=%s upper=%s lower=%s", symbol, c.upper, c.lower)
                    return c
            except Exception:
                pass

        logger.warning("CIRCUIT missing in cache symbol=%s (REST quote disabled)", symbol)
        return Circuit()

    # ================ *Order average price fetch* =================== #
    async def _get_executed_avg_price(self, order_id: str, timeout_sec: float = 3.0) -> Optional[float]:
        """
        Zerodha ke order_history se actual executed average_price nikalta hai.
        Market order me ye LTP se different ho sakta hai.
        """
        def _fetch(api_key: str, access_token: str, oid: str):
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            return kite.order_history(oid)

        t0 = time.time()
        while time.time() - t0 < timeout_sec:
            try:
                hist = await asyncio.to_thread(_fetch, self.api_key, self.access_token, order_id)
                # history list hoti hai; last state me avg_price hota hai
                if isinstance(hist, list) and hist:
                    last = hist[-1]
                    avg = last.get("average_price")
                    status = (last.get("status") or "").upper()
                    if avg and float(avg) > 0:
                        return float(avg)
                    # agar order complete hua but avg 0 aaya to retry small delay
                    if status in ("COMPLETE", "REJECTED", "CANCELLED"):
                        return float(avg) if avg else None
            except Exception:
                pass
            await asyncio.sleep(0.2)

        return None


    async def start_ladder(
        self,
        symbol: str,
        started_side: Side,
        settings_mode: SettingsMode,
        settings_payload: Optional[Dict[str, Any]] = None,
        scan_name: Optional[str] = None,
        ltp_hint: Optional[float] = None,
        open_hint: Optional[float] = None,
        alert_ts: Optional[float] = None,
        dispatch_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        """  Start ladder session for a symbol.
        Flow:
        - If KILL active -> block.
        - Merge settings (load saved + override payload, then save).
        - If already running -> return.
        - If token not available in RAM -> create WAIT_FEED session (token=0).
        - If token available:
            - create session with token
            - if LTP already present -> enter immediately
            - else WAIT_LTP (ingest_tick will enter on first tick)
        """
        symbol = (symbol or "").strip().upper()
        if not symbol:
            return {"error": "BAD_SYMBOL"}

        # Block all new ladders for the day if insufficient funds hit on first trade
        try:
            if await self.redis.get(self._insuff_halt_key()):
                logger.error("START_LADDER blocked: INSUFF_FUNDS_DAILY_HALT user=%s symbol=%s", self.user_id, symbol)
                return {"error": "INSUFFICIENT_FUNDS_DAILY_HALT"}
        except Exception:
            pass

        # 0) Kill switch guard
        if await self._is_kill():
            logger.error("START_LADDER blocked by KILL user=%s symbol=%s", self.user_id, symbol)
            await push_event(self.redis, self.user_id, symbol, "START_BLOCKED_KILL", {})
            return {"error": "KILL_SWITCH_ACTIVE"}

        started_side = (started_side or "BUY").strip().upper()  # type: ignore
        if started_side not in ("BUY", "SELL"):
            return {"error": "BAD_SIDE"}

        settings_mode = (settings_mode or "UNIVERSAL").strip().upper()  # type: ignore
        if settings_mode not in ("UNIVERSAL", "STOCK"):
            return {"error": "BAD_SETTINGS_MODE"}

        # Cache open hint from WS (no Redis dependency)
        try:
            if open_hint and float(open_hint) > 0:
                self.opens[symbol] = float(open_hint)
        except Exception:
            pass

        # 1) ✅ Already running OR completed? (Prevent duplicate ladder per session)
        existing = self.sessions.get(symbol)
        if existing:
            # If active, ladder is still running
            if existing.active:
                # Recovery path: stale WAIT state with old/missing token.
                try:
                    if existing.leg_state and existing.leg_state.status == "IDLE":
                        if existing.leg_state.reason in ("WAIT_LTP", "WAIT_FEED"):
                            latest_token = self.SYMBOL_TOKEN_RAM.get(self.user_id, {}).get(symbol) if self.SYMBOL_TOKEN_RAM else None
                            if latest_token:
                                latest_token = int(latest_token)
                                if latest_token != int(existing.token or 0):
                                    logger.info(
                                        "RECOVER_SESSION_TOKEN user=%s symbol=%s old=%s new=%s",
                                        self.user_id, symbol, existing.token, latest_token
                                    )
                                    existing.token = latest_token
                            ltp_now = float(self.ticks.get(int(existing.token or 0), 0.0) or 0.0)
                            if ltp_now > 0:
                                logger.info(
                                    "RECOVER_SESSION_ENTER user=%s symbol=%s token=%s ltp=%.2f",
                                    self.user_id, symbol, existing.token, ltp_now
                                )
                                await self._enter_leg(existing)
                                await self._snapshot(existing)
                                return {"status": "started", "session": existing.to_public_dict()}
                except Exception:
                    logger.exception("RECOVER_SESSION failed user=%s symbol=%s", self.user_id, symbol)

                logger.info("START_LADDER already_running user=%s symbol=%s", self.user_id, symbol)
                return {"status": "already_running", "session": existing.to_public_dict()}
            
            # ✅ NEW: If inactive (completed all cycles), prevent re-entry
            # This ensures only ONE ladder per stock per trading session
            else:
                logger.warning(
                    "START_LADDER blocked_duplicate user=%s symbol=%s | "
                    "Ladder already completed today. No re-entry allowed.",
                    self.user_id, symbol
                )
                await push_event(self.redis, self.user_id, symbol, "START_BLOCKED_DUPLICATE", {
                    "reason": "Ladder already completed for this symbol today"
                })
                return {
                    "error": "Ladder Already Completed Today",
                    "status": "duplicate_rejected"
                }

        # 2) Ensure RAM map exists
        if self.SYMBOL_TOKEN_RAM is None:
            self.SYMBOL_TOKEN_RAM = {}

        token_raw = self.SYMBOL_TOKEN_RAM.get(self.user_id, {}).get(symbol)
      
        # 3) Load + merge settings (Wait, logic moved up?)
        # IMPORTANT: Logic needs to load settings BEFORE logging "START LADDER" if we want to log the "Effective Side".
        
        settings = await self.load_settings(settings_mode, symbol)
        if settings_payload:
            settings = LadderSettings.from_dict({**asdict(settings), **settings_payload})
            await self.save_settings(settings_mode, symbol, settings)

        # ==========================================================
        # ✅ ENTRY THRESHOLD LOGIC (Override Side / Block)
        # Uses live ticks + cached open; strict check with ms-level retry.
        # ==========================================================
        pending_entry_threshold = False
        if settings.entry_threshold_enabled:
            try:
                ltp = 0.0
                open_price = 0.0

                # Try LTP from in-memory ticks (if token known)
                try:
                    token_guess = None
                    if getattr(self, "SYMBOL_TOKEN_RAM", None):
                        token_guess = self.SYMBOL_TOKEN_RAM.get(self.user_id, {}).get(symbol)
                    if token_guess:
                        ltp = float(self.ticks.get(int(token_guess), 0.0) or 0.0)
                except Exception:
                    pass

                # Try cached open from Redis
                try:
                    day = datetime.now().strftime("%Y%m%d")
                    raw = await self.redis.get(f"open:{day}:{symbol}")
                    if raw not in (None, ""):
                        open_price = float(raw)
                except Exception:
                    pass

                # Fast hints from caller (webhook prefilter)
                if ltp <= 0 and ltp_hint and ltp_hint > 0:
                    ltp = float(ltp_hint)
                if open_price <= 0 and open_hint and open_hint > 0:
                    open_price = float(open_hint)

                # --- ms-level retry for missing LTP/Open (strict) ---
                if open_price <= 0 or ltp <= 0:
                    t0 = time.time()
                    max_wait_sec = 0.5  # ms-level window (~500ms)
                    poll_sec = 0.05     # 50ms polling
                    while (time.time() - t0) < max_wait_sec and (open_price <= 0 or ltp <= 0):
                        await asyncio.sleep(poll_sec)
                        if ltp <= 0:
                            try:
                                token_guess = None
                                if getattr(self, "SYMBOL_TOKEN_RAM", None):
                                    token_guess = self.SYMBOL_TOKEN_RAM.get(self.user_id, {}).get(symbol)
                                if token_guess:
                                    ltp = float(self.ticks.get(int(token_guess), 0.0) or 0.0)
                            except Exception:
                                pass
                        if open_price <= 0:
                            try:
                                open_price = await self._get_open_price(symbol)
                            except Exception:
                                pass
                    if open_price <= 0 or ltp <= 0:
                        waited_ms = int((time.time() - t0) * 1000)
                        logger.warning(
                            "**ENTRY_THRESHOLD_SKIP** [%s] Missing data after %sms (Open=%s, LTP=%s) -> BLOCK",
                            symbol, waited_ms, open_price, ltp
                        )
                        # For MANUAL starts, wait for data instead of rejecting.
                        if str(scan_name or "").upper() == "MANUAL":
                            pending_entry_threshold = True
                            logger.warning(
                                "ENTRY_THRESHOLD_WAIT user=%s symbol=%s -> WAIT_LTP (manual)",
                                self.user_id, symbol
                            )
                        else:
                            await push_event(self.redis, self.user_id, symbol, "ENTRY_REJECTED", {
                                "reason": "Threshold data missing",
                                "open": open_price,
                                "ltp": ltp,
                                "wait_ms": waited_ms
                            })
                            return {
                                "error": "Entry Threshold Data Missing",
                                "status": "rejected"
                            }

                if (open_price > 0 and ltp > 0) and not pending_entry_threshold:
                    diff_pct = ((ltp - open_price) / open_price) * 100
                    thresh = settings.entry_threshold_pct
                    logger.info("ENTRY_THRESHOLD_CHECK user=%s symbol=%s open=%.2f ltp=%.2f diff=%.2f%% thresh=%.2f%% enabled=%s",
                                self.user_id, symbol, open_price, ltp, diff_pct, thresh, settings.entry_threshold_enabled)

                    if diff_pct >= thresh:
                        started_side = "BUY"
                        logger.info("**ENTRY_THRESHOLD_PASS** [%s] Open=%.2f LTP=%.2f Diff=%.2f%% >= %.2f%% => FORCE BUY", symbol, open_price, ltp, diff_pct, thresh)
                    elif diff_pct <= -thresh:
                        started_side = "SELL"
                        logger.info("**ENTRY_THRESHOLD_PASS** [%s] Open=%.2f LTP=%.2f Diff=%.2f%% <= -%.2f%% => FORCE SELL", symbol, open_price, ltp, diff_pct, thresh)
                    else:
                        # Condition not met
                        logger.warning("**ENTRY_THRESHOLD_REJECT** [%s] Open=%.2f LTP=%.2f Diff=%.2f%% | Thresh=%.2f%%", symbol, open_price, ltp, diff_pct, thresh)
                        await push_event(self.redis, self.user_id, symbol, "ENTRY_REJECTED", {
                            "reason": "Threshold not met",
                            "diff": diff_pct,
                            "thresh": thresh
                        })
                        return {
                            "error": f"Entry Threshold Not Met (Diff: {diff_pct:.2f}%)",
                            "status": "rejected"
                        }
            except Exception as e:
                logger.error("ENTRY_LOGIC [%s] Error: %s", symbol, e)
                # Fail open to avoid blocking when data unavailable
                pass

        logger.info(
            "START_LADDER user=%s symbol=%s side=%s mode=%s token=%s payload=%s",
            self.user_id, symbol, started_side, settings_mode, str(token_raw), settings_payload or {}
        )

        await push_event(self.redis, self.user_id, symbol, "START_LADDER", {
            "side": started_side,
            "mode": settings_mode,
            "token_in_ram": token_raw,
            "settings_payload": settings_payload or {}
        })

        # 3) Load + merge settings (Already done above)
        # settings variable is already populated and updated.

        # 🔥 CHECK ACTIVE POSITIONS LIMIT (per user)
        # Manual add should bypass active limit (user request)
        if str(scan_name or "").upper() == "MANUAL":
            active_limit = 0
        else:
            active_limit = settings.max_trades_per_symbol
        scan_bucket = (scan_name or "GLOBAL").strip().upper()
        scan_bucket = re.sub(r"[^A-Z0-9_-]+", "", scan_bucket) or "GLOBAL"
        active_count = 0
        for s in self.sessions.values():
            st = getattr(s, "leg_state", None)
            status = str(getattr(st, "status", "") or "").upper() if st else ""
            if getattr(s, "active", False) and status in ("RUNNING", "EXITING"):
                qty = int(getattr(st, "qty", 0) or 0) if st else 0
                if qty > 0:
                    active_count += 1
        if active_limit > 0:
            remaining = max(0, active_limit - active_count)
            logger.info("ACTIVE_LIMIT_CHECK user=%s scan=%s active=%s limit=%s remaining=%s",
                        self.user_id, scan_bucket, active_count, active_limit, remaining)
        if active_limit > 0 and active_count >= active_limit:
            if not self._global_limit_warned:
                logger.warning("START_BLOCKED_ACTIVE_LIMIT user=%s scan=%s active=%s limit=%s",
                               self.user_id, scan_bucket, active_count, active_limit)
                self._global_limit_warned = True
            return {"error": f"ACTIVE_LIMIT_REACHED {scan_bucket} ({active_count}/{active_limit})"}

        # 4) Token missing -> WAIT_FEED session
        if not token_raw:
            sess = LadderSession(
                user_id=self.user_id,
                symbol=symbol,
                token=0,  # waiting for feed
                started_side=started_side,  # type: ignore
                settings_mode=settings_mode,  # type: ignore
                settings=settings,
                cycle_index=0,
                leg=started_side,  # type: ignore
                leg_state=LegState(side=started_side, status="IDLE", reason="WAIT_FEED"),  # type: ignore
                active=True,
                created_ts=time.time(),
                updated_ts=time.time(),
                session_day=datetime.now().strftime("%Y-%m-%d"),
                alert_ts=float(alert_ts or 0.0),
            )
            sess.pending_entry_threshold = pending_entry_threshold
            if dispatch_ts:
                try:
                    latency_ms = int((time.time() - float(dispatch_ts)) * 1000)
                    logger.info("**DISPATCH_TO_START_MS** user=%s symbol=%s ms=%s", self.user_id, symbol, latency_ms)
                except Exception:
                    pass
            self.sessions[symbol] = sess
            self._waiting_symbols.add(symbol)
            await self._snapshot(sess)

            logger.warning(
            f"\033[93m⏳ WAITING FOR FEED\033[0m  "
            f"\033[1m[{symbol}]\033[0m  "
            f"USER={self.user_id}"
        )
            await push_event(self.redis, self.user_id, symbol, "WAIT_FEED", {})
            # Speed-first: place entry immediately even without feed
            if settings.fast_entry_no_ltp:
                await self._enter_leg(sess)
                await self._snapshot(sess)
                return {"status": "started", "session": sess.to_public_dict(), "fast_entry": True}
            return {
                "status": "waiting_for_feed",
                "message": "Waiting for WebSocket token + LTP...",
                "session": sess.to_public_dict(),
            }

        # 5) Token available -> create session
        token = int(token_raw)

        sess = LadderSession(
            user_id=self.user_id,
            symbol=symbol,
            token=token,
            started_side=started_side,  # type: ignore
            settings_mode=settings_mode,  # type: ignore
            settings=settings,
            cycle_index=0,
            leg=started_side,  # type: ignore
            leg_state=LegState(side=started_side, status="IDLE", reason="WAIT_LTP"),  # type: ignore
            active=True,
            created_ts=time.time(),
            updated_ts=time.time(),
            session_day=datetime.now().strftime("%Y-%m-%d"),
            alert_ts=float(alert_ts or 0.0),
        )
        sess.pending_entry_threshold = pending_entry_threshold
        if dispatch_ts:
            try:
                latency_ms = int((time.time() - float(dispatch_ts)) * 1000)
                logger.info("**DISPATCH_TO_START_MS** user=%s symbol=%s ms=%s", self.user_id, symbol, latency_ms)
            except Exception:
                pass
        self.sessions[symbol] = sess
        self._register_session_token(sess)
        self._waiting_symbols.add(symbol)
        await self._snapshot(sess)

        # 6) If LTP already present -> enter instantly
        ltp = float(self.ticks.get(token, 0.0) or 0.0)

        if ltp > 0:
            open_pct_str = ""
            try:
                opn = await self._get_open_price(symbol)
                if opn > 0:
                    open_pct = ((ltp - opn) / opn) * 100.0
                    open_pct_str = f" OPEN%={open_pct:.2f} TH={settings.entry_threshold_pct:.2f}"
            except Exception:
                pass
            logger.info(
                "⚡ INSTANT ENTRY  "
                + f"{_color_side_word(started_side)} "
                + f"[{symbol}]  "
                + f"LTP={ltp:.2f}{open_pct_str}"
            )
            await self._enter_leg(sess)
            await self._snapshot(sess)
            return {"status": "started", "session": sess.to_public_dict()}

        # else wait for first tick (or speed-first entry)
        if settings.fast_entry_no_ltp:
            await self._enter_leg(sess)
            await self._snapshot(sess)
            return {"status": "started", "session": sess.to_public_dict(), "fast_entry": True}
        return {
            "status": "waiting_for_ltp",
            "message": "Token ready, waiting for first tick...",
            "session": sess.to_public_dict(),
        }
    

    async def squareoff_symbol(self, symbol: str, reason: str = "MANUAL_SQUAREOFF") -> Dict[str, Any]:
        symbol = symbol.strip().upper()
        sess = self.sessions.get(symbol)
        if not sess or not sess.active:
            logger.warning("SQUAREOFF not_running user=%s symbol=%s", self.user_id, symbol)
            return {"status": "not_running"}

        logger.warning("SQUAREOFF user=%s symbol=%s reason=%s", self.user_id, symbol, reason)
        await push_event(self.redis, self.user_id, symbol, "SQUAREOFF_REQ", {"reason": reason})

        await self._exit_leg(sess, reason=reason)
        sess.active = False
        self._unregister_symbol(sess.symbol)
        sess.updated_ts = time.time()
        await self._snapshot(sess)

        return {"status": "squareoff_done", "session": sess.to_public_dict()}

    async def kill_all(self) -> Dict[str, Any]:
        logger.error("KILL_ALL user=%s", self.user_id)
        await self.set_kill(True)

        tasks = []
        for sym, sess in list(self.sessions.items()):
            if sess.active and sess.leg_state and sess.leg_state.qty > 0:
                tasks.append(self.squareoff_symbol(sym, reason="KILL_SWITCH"))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        await push_event(self.redis, self.user_id, "-", "KILL_ALL_DONE", {"count": len(tasks)})
        await self.redis.delete(f"ws:{self.user_id}:tokens")
        return {"status": "killed", "count": len(tasks)}

    async def list_sessions(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        for sess in self.sessions.values():
            ltp = self.ticks.get(sess.token, 0.0)

            # update pnl & extremes
            st = sess.leg_state
            if st and ltp and st.qty > 0:
                if st.side == "BUY":
                    st.unrealized_pnl = round((ltp - st.avg_price) * st.qty, 2)
                    st.highest = max(st.highest, ltp)
                else:
                    st.unrealized_pnl = round((st.avg_price - ltp) * st.qty, 2)
                    st.lowest = min(st.lowest, ltp) if st.lowest else ltp

            d = sess.to_public_dict()
            d["ltp"] = ltp if ltp else None
            out.append(d)

        return out

    def _get_cycle_hist(self, sess: LadderSession) -> Dict[str, Any]:
        key = str(sess.cycle_index + 1)
        if key not in sess.cycle_hist:
            sess.cycle_hist[key] = {
                "B": {"est": [], "exec": []},
                "S": {"est": [], "exec": []},
                "tsl_est": {"B": None, "S": None},
                "tsl_exec": {"B": None, "S": None},
            }
        return sess.cycle_hist[key]


    # ------------ internal: leg entry/exit ------------
    def _calc_step_qty(self, settings: LadderSettings, ltp: float) -> int:
        if ltp <= 0:
            return 0
        if settings.qty_mode == "QTY":
            return int(max(1, settings.per_trade_qty))
        return int(max(1, math.floor(settings.per_trade_capital / ltp)))

    async def _enter_leg(self, sess: LadderSession) -> None:
        if sess.leg_state and sess.leg_state.status in ("RUNNING", "EXITING"):
            return
        if not sess.active:
            return
        symbol = sess.symbol
        s = sess.settings
        ltp = self.ticks.get(sess.token, 0.0)
        # If entry threshold data was missing on manual start, wait until we have data.
        if sess.pending_entry_threshold:
            try:
                open_price = await self._get_open_price(symbol)
            except Exception:
                open_price = 0.0
            if not ltp or ltp <= 0 or open_price <= 0:
                if sess.leg_state:
                    sess.leg_state.status = "IDLE"
                    sess.leg_state.reason = "WAIT_LTP"
                self._waiting_symbols.add(symbol)
                return
            diff_pct = ((ltp - open_price) / open_price) * 100.0
            thresh = s.entry_threshold_pct
            if diff_pct >= thresh:
                sess.leg = "BUY"
            elif diff_pct <= -thresh:
                sess.leg = "SELL"
            else:
                if sess.leg_state:
                    sess.leg_state.status = "REJECTED"
                    sess.leg_state.reason = "ENTRY_THRESHOLD_NOT_MET"
                sess.active = False
                self._unregister_symbol(sess.symbol)
                await self._snapshot(sess)
                await push_event(self.redis, self.user_id, symbol, "ENTRY_REJECTED", {
                    "reason": "Threshold not met",
                    "diff": diff_pct,
                    "thresh": thresh
                })
                return
            sess.pending_entry_threshold = False

        self._waiting_symbols.discard(sess.symbol)
        side = sess.leg
        logger.info("ENTER_LEG user=%s symbol=%s side=%s token=%s ltp=%s",
                    self.user_id, symbol, side, sess.token, ltp)
        await push_event(self.redis, self.user_id, symbol, "ENTER_LEG", {"side": side, "token": sess.token, "ltp": ltp})

        fast_entry = False
        ltp_for_order = float(ltp or 0.0)
        qty = 0

        if not ltp or ltp <= 0:
            if s.fast_entry_no_ltp:
                qty = int(max(1, int(s.per_trade_qty or 1)))
                fast_entry = True
                ltp_for_order = 0.0
                logger.warning("FAST_ENTRY_NO_LTP user=%s symbol=%s qty=%s mode=%s",
                               self.user_id, symbol, qty, s.qty_mode)
                await push_event(self.redis, self.user_id, symbol, "FAST_ENTRY_NO_LTP",
                                 {"qty": qty, "mode": s.qty_mode})
            else:
                sess.leg_state.status = "ERROR"
                sess.leg_state.reason = "NO_LTP"
                logger.error("NO_LTP user=%s symbol=%s token=%s", self.user_id, symbol, sess.token)
                await push_event(self.redis, self.user_id, symbol, "ERROR_NO_LTP", {"token": sess.token})
                await self._snapshot(sess)
                return

        if qty <= 0:
            qty = self._calc_step_qty(s, ltp) if (ltp and ltp > 0) else 0
        if qty <= 0:
            sess.leg_state.status = "ERROR"
            sess.leg_state.reason = "BAD_QTY"
            logger.error("BAD_QTY user=%s symbol=%s ltp=%s settings=%s", self.user_id, symbol, ltp, asdict(s))
            await push_event(self.redis, self.user_id, symbol, "ERROR_BAD_QTY", {"ltp": ltp, "settings": asdict(s)})
            await self._snapshot(sess)
            return

        # Store expected entry price (for slippage view)
        try:
            if ltp_for_order and ltp_for_order > 0:
                sess.leg_state.entry_price_internal = float(ltp_for_order)
        except Exception:
            pass

        lock = await self._lock(symbol, "entry", ttl_ms=1200)
        if lock != 1:
            sess.leg_state.status = "ERROR" if lock == -2 else sess.leg_state.status
            sess.leg_state.reason = "KILLED" if lock == -2 else "ENTRY_LOCKED"
            logger.warning("ENTRY_BLOCKED user=%s symbol=%s reason=%s", self.user_id, symbol, sess.leg_state.reason)
            await push_event(self.redis, self.user_id, symbol, "ENTRY_BLOCKED", {"reason": sess.leg_state.reason})
            await self._snapshot(sess)
            return

        sess.leg_state = LegState(side=side, status="RUNNING", qty=0, avg_price=0.0, entries=0)
        sess.updated_ts = time.time()

        order_sent_ts = time.time()
        try:
            oid = await self._place_mis_market(
                symbol=symbol,
                side=side,
                qty=qty,
                ltp=ltp_for_order,
                order_tag="ENTRY",
                leg=sess.leg,
            )
        except Exception as e:
            logger.error(f"ENTRY_FAILED user={self.user_id} symbol={symbol} err={e}")
            if self._is_insufficient(str(e)):
                await self._set_insuff_halt(str(e))
            sess.leg_state.status = "REJECTED"
            sess.leg_state.reason = str(e)
            sess.active = False
            self._unregister_symbol(sess.symbol)
            
            # Rejected trades count as ONE attempt. We do not retry.
            # So we do NOT decrement the global count.

            await self._snapshot(sess)
            return

        sess.leg_state.entry_order_id = str(oid)
        sess.leg_state.pending_qty = int(qty)

        exec_avg = await self._get_executed_avg_price(str(oid))
        base_price = exec_avg if (exec_avg and exec_avg > 0) else 0.0

        if base_price > 0:
            logger.info(
                "ENTRY_FILL user=%s symbol=%s oid=%s ltp=%.2f exec_avg=%s base=%.2f",
                self.user_id, symbol, oid, float(ltp), str(exec_avg), float(base_price)
            )
            sess.leg_state.qty = qty
            sess.leg_state.avg_price = float(base_price)         # ✅ real base
            sess.leg_state.entries = 1
            sess.leg_state.last_add_price = float(base_price)    # ✅ threshold base
            sess.leg_state.highest = float(base_price)
            sess.leg_state.lowest = float(base_price)
            if sess.leg_state.entry_price_internal <= 0:
                sess.leg_state.entry_price_internal = float(base_price)
            sess.leg_state.entry_price_exec = float(base_price)
            sess.leg_state.entry_filled = True
            sess.leg_state.pending_qty = 0
        else:
            est_price = float(ltp) if (ltp and ltp > 0) else 0.0
            sess.leg_state.entry_price_internal = float(est_price)
            sess.leg_state.entry_price_exec = 0.0
            sess.leg_state.entry_filled = False
            logger.warning(
                "ENTRY_PENDING user=%s symbol=%s oid=%s ltp=%.2f exec_avg=%s",
                self.user_id, symbol, oid, float(ltp), str(exec_avg)
            )

        # Always record ENTRY row so C1B1/C1EB1 stay aligned
        if not sess.leg_state.entry_hist_recorded:
            hist = self._get_cycle_hist(sess)
            side_key = "B" if side == "BUY" else "S"
            est_list = hist[side_key]["est"]
            exec_list = hist[side_key]["exec"]
            est_val = float(sess.leg_state.entry_price_internal or 0.0)
            exec_val = float(sess.leg_state.entry_price_exec or 0.0)
            # Reserve slot for entry even if exec not known yet
            est_list.append(est_val)
            exec_list.append(exec_val)
            sess.leg_state.entry_hist_recorded = True
        else:
            # If entry already recorded, update exec when available
            try:
                hist = self._get_cycle_hist(sess)
                side_key = "B" if side == "BUY" else "S"
                exec_list = hist[side_key]["exec"]
                if exec_list and sess.leg_state.entry_price_exec:
                    exec_list[0] = float(sess.leg_state.entry_price_exec)
            except Exception:
                pass

        # Entry slippage log (expected vs actual)
        try:
            exp = float(sess.leg_state.entry_price_internal or 0.0)
            exe = float(sess.leg_state.entry_price_exec or 0.0)
            if exp > 0 and exe > 0:
                slip = exe - exp
                logger.info(
                    "ENTRY SLIP  %s [%s] EXP=%.2f EXEC=%.2f DIFF=%.2f",
                    _color_side_word(side), symbol, exp, exe, slip
                )
        except Exception:
            pass

        sess.leg_state.reason = ""

        entry_ltp = float(ltp) if (ltp and ltp > 0) else float(sess.leg_state.entry_price_exec or 0.0)
        open_pct_str = ""
        try:
            opn = await self._get_open_price(symbol)
            if opn > 0 and entry_ltp > 0:
                open_pct = ((entry_ltp - opn) / opn) * 100.0
                open_pct_str = f" open%={open_pct:.2f} th={settings.entry_threshold_pct:.2f}"
        except Exception:
            pass
        logger.info("ENTRY_PLACED user=%s symbol=%s side=%s qty=%s ltp=%s oid=%s%s",
                    self.user_id, symbol, side, qty, entry_ltp, oid, open_pct_str)
        try:
            if order_sent_ts:
                logger.info("**ORDER_SEND_TO_PLACED_MS** user=%s symbol=%s ms=%s",
                            self.user_id, symbol, int((time.time() - order_sent_ts) * 1000))
        except Exception:
            pass
        if sess.alert_ts and sess.alert_ts > 0:
            try:
                latency_ms = int((time.time() - float(sess.alert_ts)) * 1000)
                logger.info("**ALERT_TO_ENTRY_MS** user=%s symbol=%s ms=%s", self.user_id, symbol, latency_ms)
            except Exception:
                pass
        # Single-line timing summary (webhook -> subscribe -> first tick -> entry)
        try:
            tkey = f"timing:{self.user_id}:{symbol}"
            tdata = await self.redis.hgetall(tkey)
            if tdata:
                wb = float(tdata.get("webhook_ts") or 0.0)
                sub = float(tdata.get("subscribe_ts") or 0.0)
                tick = float(tdata.get("tick_ts") or 0.0)
                now_ts = time.time()
                wb_to_sub = int((sub - wb) * 1000) if (wb > 0 and sub > 0) else -1
                sub_to_tick = int((tick - sub) * 1000) if (sub > 0 and tick > 0) else -1
                alert_to_entry = int((now_ts - float(sess.alert_ts)) * 1000) if sess.alert_ts else -1
                logger.info(
                    "TIMING user=%s symbol=%s webhook->sub=%sms sub->tick=%sms alert->entry=%sms",
                    self.user_id, symbol, wb_to_sub, sub_to_tick, alert_to_entry
                )
        except Exception:
            pass
        await push_event(self.redis, self.user_id, symbol, "ENTRY_PLACED",
                         {"side": side, "qty": qty, "ltp": entry_ltp, "order_id": oid, "fast_entry": fast_entry, "pending": (not sess.leg_state.entry_filled)})

        await self._snapshot(sess)

        asyncio.create_task(self.get_circuit(symbol), name=f"circuit_prefetch:{self.user_id}:{symbol}")


    # async def _exit_leg(self, sess: LadderSession, reason: str) -> None:
    #     symbol = sess.symbol
    #     st = sess.leg_state
    #     if not st or st.status not in ("RUNNING", "EXITING"):
    #         return

    #     if st.qty <= 0:
    #         st.status = "DONE"
    #         st.reason = reason
    #         logger.info("EXIT_SKIP qty=0 user=%s symbol=%s reason=%s", self.user_id, symbol, reason)
    #         await push_event(self.redis, self.user_id, symbol, "EXIT_SKIP_QTY0", {"reason": reason})
    #         return

    #     lock = await self._lock(symbol, "exit", ttl_ms=1500)
    #     if lock != 1:
    #         st.reason = "KILLED" if lock == -2 else "EXIT_LOCKED"
    #         st.status = "ERROR" if lock == -2 else st.status
    #         logger.warning("EXIT_BLOCKED user=%s symbol=%s reason=%s", self.user_id, symbol, st.reason)
    #         await push_event(self.redis, self.user_id, symbol, "EXIT_BLOCKED", {"reason": st.reason})
    #         await self._snapshot(sess)
    #         return

    #     logger.warning("EXIT_TRIGGER user=%s symbol=%s reason=%s side=%s qty=%s avg=%s",
    #                    self.user_id, symbol, reason, st.side, st.qty, st.avg_price)
    #     await push_event(self.redis, self.user_id, symbol, "EXIT_TRIGGER",
    #                      {"reason": reason, "side": st.side, "qty": st.qty, "avg": st.avg_price})

    #     st.status = "EXITING"
    #     st.reason = reason
    #     await self._snapshot(sess)

    #     exit_side: Side = "SELL" if st.side == "BUY" else "BUY"
    #     qty = int(st.qty)
    #     oid = await self._place_mis_market(symbol=symbol, side=exit_side, qty=qty)

    #     logger.info("EXIT_PLACED user=%s symbol=%s exit_side=%s qty=%s oid=%s",
    #                 self.user_id, symbol, exit_side, qty, oid)
    #     await push_event(self.redis, self.user_id, symbol, "EXIT_PLACED",
    #                      {"exit_side": exit_side, "qty": qty, "order_id": oid})

    #     st.status = "DONE"
    #     st.qty = 0
    #     await self._snapshot(sess)

    #     logger.info("LEG_DONE user=%s symbol=%s reason=%s", self.user_id, symbol, reason)
    #     await push_event(self.redis, self.user_id, symbol, "LEG_DONE", {"reason": reason})

    async def _exit_leg(self, sess: LadderSession, reason: str, tsl_est: Optional[float] = None) -> None:
        symbol = sess.symbol
        st = sess.leg_state

        # ✅ EXIT only once: if already EXITING/DONE, do nothing
        if not st or st.status != "RUNNING":
            return

        # ✅ mark EXITING immediately (race close)
        st.status = "EXITING"
        st.reason = reason
        await self._snapshot(sess)

        # ✅ redis lock (extra safety across tasks/process)
        lock = await self._lock(symbol, "exit", ttl_ms=1500)
        if lock == 0:
            # someone else already exiting
            return
        if lock == -2:
            st.status = "ERROR"
            st.reason = "KILLED"
            await push_event(self.redis, self.user_id, symbol, "EXIT_BLOCKED", {"reason": "KILLED"})
            await self._snapshot(sess)
            return

        if st.qty <= 0:
            st.status = "DONE"
            st.reason = reason
            await push_event(self.redis, self.user_id, symbol, "EXIT_SKIP_QTY0", {"reason": reason})
            await self._snapshot(sess)
            return

        logger.warning(
            "EXIT_TRIGGER user=%s symbol=%s reason=%s side=%s qty=%s avg=%s",
            self.user_id, symbol, reason, st.side, st.qty, st.avg_price
        )
        await push_event(self.redis, self.user_id, symbol, "EXIT_TRIGGER",
                        {"reason": reason, "side": st.side, "qty": st.qty, "avg": st.avg_price})

        exit_side: Side = "SELL" if st.side == "BUY" else "BUY"
        qty = int(st.qty)

        oid = await self._place_mis_market(
            symbol=symbol,
            side=exit_side,
            qty=qty,
            enforce_price_band=False,
            order_tag="EXIT",
            reason=reason,
            leg=sess.leg,
        )
        st.exit_order_id = str(oid)

        logger.info("EXIT_PLACED user=%s symbol=%s exit_side=%s qty=%s oid=%s",
                    self.user_id, symbol, exit_side, qty, oid)
        await push_event(self.redis, self.user_id, symbol, "EXIT_PLACED",
                        {"exit_side": exit_side, "qty": qty, "order_id": oid, "reason": reason})

        # capture executed exit price for UI (best-effort)
        try:
            exec_avg = await self._get_executed_avg_price(str(oid))
            if exec_avg and exec_avg > 0:
                st.exit_price_exec = float(exec_avg)
            else:
                st.exit_price_exec = float(self.ticks.get(sess.token, 0.0) or 0.0)
        except Exception:
            st.exit_price_exec = float(self.ticks.get(sess.token, 0.0) or 0.0)

        if reason == "TRAILING_SL":
            hist = self._get_cycle_hist(sess)
            side_key = "B" if st.side == "BUY" else "S"
            if tsl_est is not None:
                hist["tsl_est"][side_key] = float(tsl_est)
            hist["tsl_exec"][side_key] = float(st.exit_price_exec or 0.0)

        # ✅ mark done
        st.status = "DONE"
        st.qty = 0
        await self._snapshot(sess)

        logger.info("LEG_DONE user=%s symbol=%s reason=%s", self.user_id, symbol, reason)
        await push_event(self.redis, self.user_id, symbol, "LEG_DONE", {"reason": reason})


    
    async def _place_mis_market(
        self,
        symbol: str,
        side: Side,
        qty: int,
        ltp: float = 0.0,
        enforce_price_band: bool = False,
        order_tag: str = "ENTRY",
        reason: Optional[str] = None,
        leg: Optional[str] = None,
    ) -> str:
        """Place MIS market order via Kite in the order worker. Verified Capital check if ltp > 0."""
        def _place(api_key: str, access_token: str, sym: str, s: Side, q: int, ref_ltp: float, enforce_band: bool) -> str:
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            
            # ✅ Price band check (user request) — only for entry/add, not exits
            if enforce_band and ref_ltp > 0:
                if ref_ltp < 100 or ref_ltp > 4000:
                    raise Exception(f"Price outside allowed range (100-4000): {ref_ltp:.2f}")

            # ✅ Capital Check (User Request)
            if ref_ltp > 0:
                try:
                    # Fetch equity margins
                    # NOTE: kite.margins(segment="equity") returns the equity object DIRECTLY (flattened),
                    # whereas kite.margins() returns {"equity": {...}, "commodity": {...}}
                    m = kite.margins(segment="equity")
                    
                    # Robust extraction:
                    eq_data = m.get("equity") or m
                    avail = float(eq_data.get("available", {}).get("live_balance") or 0)
                    
                    # Approx MIS margin (assuming 5x starts -> 20%). 
                    # We use a conservative 25% to be safe, or just 20%.
                    # Value = Price * Qty. Required = Value / 5.
                    val = ref_ltp * q
                    req = val / 5.0
                    
                    if avail < req:
                        raise Exception(f"Insufficient Capital: Avail={avail:.2f} Req~={req:.2f} (Val={val:.2f})")
                    
                except Exception as e:
                    # If it's the insufficient capital exception, re-raise it. 
                    # If it's a network error fetching margins, we arguably should strictly fail too per user request.
                    raise e

            return kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=sym,
                transaction_type=(kite.TRANSACTION_TYPE_BUY if s == "BUY" else kite.TRANSACTION_TYPE_SELL),
                quantity=int(q),
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
            )
        try:
            tag = (order_tag or "ENTRY").upper()
            leg_str = f" LEG={leg}" if leg else ""
            reason_str = f" REASON={reason}" if reason else ""
            logger.info(
    "📤 ORDER SENT  "
    + f"[{tag}] "
    + f"{_color_side_word(side)} "
    + f"[{symbol}]  "
    + f"QTY={qty}{leg_str}{reason_str}  USER={self.user_id}  "
    + f"TIME={datetime.now().strftime('%H:%M:%S')}"
)
            t0 = time.perf_counter()
            oid = await self.order_worker.submit(
                _place,
                api_key=self.api_key,
                access_token=self.access_token,
                sym=symbol,
                s=side,
                q=qty,
                ref_ltp=ltp,
                enforce_band=enforce_price_band,
            )
            latency = (time.perf_counter() - t0) * 1000
            tag = (order_tag or "ENTRY").upper()
            leg_str = f" LEG={leg}" if leg else ""
            reason_str = f" REASON={reason}" if reason else ""
            logger.info(
            "✅ ORDER PLACED  "
            + f"[{tag}] "
            + f"{_color_side_word(side)} "
            + f"[{symbol}]  "
            + f"QTY={qty}{leg_str}{reason_str}  "
            + f"OID={oid}  "
            + f"LATENCY={latency:.1f}ms  "
            + f"TIME={datetime.now().strftime('%H:%M:%S')}"
        )
            return str(oid)
        except Exception as e:
            emsg = str(e)
            if "MIS orders are currently blocked" in emsg:
                logger.error(
                    "❌ ORDER FAILED  "
                    + f"{_color_side_word(side)} "
                    + f"[{symbol}]  "
                    + f"QTY={qty}  ERR=MisBlocked(CNC only)  "
                    + f"TIME={datetime.now().strftime('%H:%M:%S')}"
                )
            else:
                logger.error(
                    "❌ ORDER FAILED  "
                    + f"{_color_side_word(side)} "
                    + f"[{symbol}]  "
                    + f"QTY={qty}  ERR={e}  "
                    + f"TIME={datetime.now().strftime('%H:%M:%S')}",
                    exc_info=True
                )

            await push_event(self.redis, self.user_id, symbol, "ORDER_FAILED",
                             {"side": side, "qty": qty, "err": str(e)})
            raise

    async def ingest_tick(self, token: int, ltp: float, open_price: Optional[float] = None) -> None:
        """Called by FastAPI (in-memory). NO Redis here."""
        if not self._running:
            return
        try:
            token = int(token)
            ltp = float(ltp)
            if ltp <= 0:
                return

            self.ticks[token] = ltp

            # Cache open from ticks (symbol-level)
            try:
                opn = float(open_price or 0.0)
            except Exception:
                opn = 0.0
            if opn > 0:
                for sym in list(self._token_sessions.get(token, set())):
                    self.opens[sym] = opn

            # evaluate only sessions that match this token
            await self._eval_token(token)

            # if any session is waiting for LTP, try to enter (limited to waiting set)
            for sym in list(self._waiting_symbols):
                sess = self.sessions.get(sym)
                if not sess or not sess.active:
                    self._waiting_symbols.discard(sym)
                    continue
                # Assign token if waiting for feed
                if sess.token == 0:
                    t = self.SYMBOL_TOKEN_RAM.get(self.user_id, {}).get(sess.symbol)
                    if t:
                        sess.token = int(t)
                        self._register_session_token(sess)
                        logger.info("FEED_READY user=%s symbol=%s token=%s", self.user_id, sess.symbol, sess.token)
                        if opn > 0:
                            self.opens[sess.symbol] = opn

                # If this tick matches session, start ladder
                if sess.token == token and sess.leg_state and sess.leg_state.status == "IDLE":
                    if sess.leg_state.reason in ("WAIT_LTP", "WAIT_FEED"):
                        await self._enter_leg(sess)
                        await self._snapshot(sess)
                        self._waiting_symbols.discard(sym)


        except Exception:
            # never crash hot path
            return

    async def handle_order_update(self, payload: Dict[str, Any]) -> None:
        """
        Called when we receive an order update via WebSocket Ingest.
        Payload struct: { "order_id": "...", "status": "REJECTED", "symbol": "...", "tradingsymbol": "...", "status_message": "..." }
        """
        # Symbol might be in payload as 'tradingsymbol' or 'symbol'
        symbol = (payload.get("tradingsymbol") or payload.get("symbol") or "").strip().upper()
        status = str(payload.get("status") or "").strip().upper()
        reason = str(payload.get("status_message") or payload.get("reason") or "")
        order_id = str(payload.get("order_id") or payload.get("orderId") or payload.get("order_id") or "")
        txn_type = str(payload.get("transaction_type") or payload.get("transactionType") or "").strip().upper()
        avg_price_raw = payload.get("average_price") or payload.get("averagePrice") or payload.get("avg_price")
        filled_qty_raw = payload.get("filled_quantity") or payload.get("filledQuantity") or payload.get("quantity")

        try:
            avg_price = float(avg_price_raw or 0)
        except Exception:
            avg_price = 0.0
        try:
            filled_qty = int(filled_qty_raw or 0)
        except Exception:
            filled_qty = 0
        
        if not symbol:
            return

        sess = self.sessions.get(symbol)
        if not sess:
            return

        # Handle Rejection / Failure
        if status in ("REJECTED", "CANCELLED", "FAILURE"):
            logger.warning(f"❌ ORDER {status} user={self.user_id} symbol={symbol} reason={reason}")

            is_entry_order = bool(order_id and sess.leg_state and order_id == sess.leg_state.entry_order_id)
            is_exit_order = bool(order_id and sess.leg_state and order_id == sess.leg_state.exit_order_id)

            if sess.leg_state:
                # Only update if currently running/idle to avoid overwriting final states
                if sess.leg_state.status in ("RUNNING", "IDLE"):
                    sess.leg_state.status = "REJECTED"
                    sess.leg_state.reason = reason or "Order Rejected"
                    if is_entry_order:
                        if self._is_insufficient(reason):
                            await self._set_insuff_halt(reason or "INSUFFICIENT_FUNDS")
                        sess.leg_state.entry_filled = False
                        sess.leg_state.qty = 0
                        sess.leg_state.avg_price = 0.0
                        sess.leg_state.pending_qty = 0
                    if is_exit_order:
                        # If exit order rejected, keep session active so exits keep monitoring
                        sess.leg_state.status = "RUNNING"

            # Only stop session on ENTRY rejection
            if is_entry_order:
                sess.active = False
                self._unregister_symbol(sess.symbol)

            sess.updated_ts = time.time()
            await self._snapshot(sess)
            await push_event(self.redis, self.user_id, symbol, "ORDER_UPDATE", {
                "status": status,
                "reason": reason,
                "order_id": order_id
            })
            return

        # Handle Successful Fill
        if status in ("COMPLETE", "FILLED"):
            if not sess.leg_state:
                return

            st = sess.leg_state
            if order_id and st.entry_order_id == order_id and not st.entry_filled:
                prev_qty = int(st.qty or 0)
                fill_price = avg_price if avg_price > 0 else float(self.ticks.get(sess.token, 0.0) or 0.0)
                if filled_qty <= 0:
                    filled_qty = st.pending_qty
                if fill_price > 0 and filled_qty > 0:
                    st.qty = int(filled_qty)
                    st.avg_price = float(fill_price)
                    st.entries = max(1, int(st.entries or 0))
                    st.last_add_price = float(fill_price)
                    st.highest = float(fill_price)
                    st.lowest = float(fill_price)
                    st.entry_price_exec = float(fill_price)
                    if st.entry_price_internal <= 0:
                        st.entry_price_internal = float(fill_price)
                    st.entry_filled = True
                    st.pending_qty = 0
                    st.reason = ""
                    logger.info(
                        "ENTRY_FILL_WS user=%s symbol=%s oid=%s side=%s qty=%s->%s avg=%s",
                        self.user_id, symbol, order_id, st.side, prev_qty, st.qty, float(fill_price)
                    )
                    if not st.entry_hist_recorded:
                        hist = self._get_cycle_hist(sess)
                        side_key = "B" if st.side == "BUY" else "S"
                        # Avoid duplicate entry rows (can be recorded earlier at placement)
                        est_list = hist[side_key]["est"]
                        exec_list = hist[side_key]["exec"]
                        est_val = float(st.entry_price_internal)
                        exec_val = float(st.entry_price_exec)
                        est_list.append(est_val)
                        exec_list.append(exec_val)
                        st.entry_hist_recorded = True
                    else:
                        # Update last exec with actual fill price if already recorded
                        try:
                            hist = self._get_cycle_hist(sess)
                            side_key = "B" if st.side == "BUY" else "S"
                            exec_list = hist[side_key].get("exec") or []
                            if exec_list:
                                exec_list[0] = float(st.entry_price_exec)
                        except Exception:
                            pass
                    sess.updated_ts = time.time()
                    await self._snapshot(sess)
                    await push_event(self.redis, self.user_id, symbol, "ENTRY_FILL_WS", {
                        "order_id": order_id,
                        "avg_price": fill_price,
                        "qty": st.qty
                    })
            elif order_id and st.exit_order_id == order_id:
                prev_qty = int(st.qty or 0)
                if avg_price > 0:
                    st.exit_price_exec = float(avg_price)
                st.qty = 0
                st.status = "DONE"
                # If exit was TSL, store executed TSL price on fill
                if st.reason == "TRAILING_SL":
                    try:
                        hist = self._get_cycle_hist(sess)
                        side_key = "B" if st.side == "BUY" else "S"
                        hist["tsl_exec"][side_key] = float(st.exit_price_exec or avg_price or 0.0)
                    except Exception:
                        pass
                logger.info(
                    "EXIT_FILL_WS user=%s symbol=%s oid=%s qty=%s->0 avg=%s",
                    self.user_id, symbol, order_id, prev_qty, float(avg_price or 0.0)
                )
                sess.updated_ts = time.time()
                await self._snapshot(sess)
                await push_event(self.redis, self.user_id, symbol, "EXIT_FILL_WS", {
                    "order_id": order_id,
                    "avg_price": avg_price,
                    "qty": filled_qty
                })
            elif order_id and st.last_add_order_id == order_id:
                # Update last ADD exec price to exact fill
                try:
                    hist = self._get_cycle_hist(sess)
                    side_key = "B" if st.side == "BUY" else "S"
                    exec_list = hist[side_key].get("exec") or []
                    if exec_list:
                        fill_price = avg_price if avg_price > 0 else float(self.ticks.get(sess.token, 0.0) or 0.0)
                        exec_list[-1] = float(fill_price)
                except Exception:
                    pass
                st.last_add_order_id = ""


    async def _poller(self) -> None:
        """
        Polls active sessions for:
        - periodic PnL update
        - fallback LTP fetch
        Also enforces kill switch.
        """
        try:
            while self._running:
                if await self._is_kill():
                    if not self._kill_handled:
                        self._kill_handled = True
                        logger.error("KILL detected in poller user=%s -> kill_all() ONCE", self.user_id)
                        await self.kill_all()
                    else:
                        # kill already handled, just keep engine in safe mode
                        await asyncio.sleep(1.0)
                    continue

                for sym, sess in list(self.sessions.items()):
                    if not sess.active or not sess.leg_state or sess.leg_state.status != "RUNNING":
                        continue
                    await self._update_pnl(sess)

                await asyncio.sleep(0.25)  # ~4Hz

        except asyncio.CancelledError:
            return
        except Exception:
            logger.error("❌ POLLER_CRASH user_id=%s", self.user_id, exc_info=True)
            await push_event(self.redis, self.user_id, "-", "POLLER_CRASH", {})

    async def _eval_token(self, token: int) -> None:
        syms = self._token_sessions.get(int(token))
        if not syms:
            return
        for sym in list(syms):
            sess = self.sessions.get(sym)
            if not sess or not sess.active:
                self._unregister_symbol(sym)
                continue
            await self._eval_session(sess)

    async def _start_next_leg_or_finish(self, sess: LadderSession) -> None:
        """ After a leg completes, either start the opposite leg (same cycle), or advance cycle, or finish session."""
        if not sess.active:
            return
        if sess.stop_after_exit:
            sess.active = False
            self._unregister_symbol(sess.symbol)
            if sess.leg_state:
                sess.leg_state.status = "COMPLETE"
                sess.leg_state.reason = "INSUFFICIENT_FUNDS"
            sess.updated_ts = time.time()
            await self._snapshot(sess)
            await push_event(self.redis, self.user_id, sess.symbol, "SESSION_DONE", {"reason": "INSUFFICIENT_FUNDS"})
            return

        # If we just finished BUY, go to SELL; if finished SELL, cycle completed.
        if sess.leg == "BUY":
            sess.leg = "SELL"
        else:
            sess.cycle_index += 1
            if sess.cycle_index >= sess.settings.ladder_cycles:
                sess.active = False
                self._unregister_symbol(sess.symbol)
                # Explicitly mark as COMPLETE for UI
                if sess.leg_state:
                    sess.leg_state.status = "COMPLETE"
                    sess.leg_state.reason = "CYCLES_DONE"
                
                sess.updated_ts = time.time()
                await self._snapshot(sess)
                logger.info("SESSION_DONE user=%s symbol=%s cycles=%s",
                            self.user_id, sess.symbol, sess.settings.ladder_cycles)
                logger.info("CYCLE_COMPLETE user=%s symbol=%s cycles=%s",
                            self.user_id, sess.symbol, sess.settings.ladder_cycles)
                await push_event(self.redis, self.user_id, sess.symbol, "SESSION_DONE", {})
                return
            sess.leg = "BUY"

        # Prepare next leg as IDLE waiting for LTP
        sess.leg_state = LegState(side=sess.leg, status="IDLE", reason="WAIT_LTP")
        sess.updated_ts = time.time()
        await self._snapshot(sess)
        self._waiting_symbols.add(sess.symbol)

        # If LTP exists, enter now
        ltp = self.ticks.get(sess.token, 0.0)
        if ltp > 0:
            await self._enter_leg(sess)
            await self._snapshot(sess)


    async def _eval_session(self, sess: LadderSession) -> None:
        st = sess.leg_state
        if not st or st.status != "RUNNING":
            return

        ltp = self.ticks.get(sess.token, 0.0)
        if not ltp or ltp <= 0:
            return

        # ✅ Guard: do not evaluate exits/adds until entry is actually filled
        if not st.entry_filled or st.qty <= 0 or st.avg_price <= 0:
            return

        s = sess.settings
        sym = sess.symbol

        # circuit snapshot refresh (background)
        circuit = self.circuits.get(sym)
        if circuit is None or (time.time() - circuit.ts) > 900:
            asyncio.create_task(self.get_circuit(sym), name=f"circuit_refresh:{self.user_id}:{sym}")
            circuit = self.circuits.get(sym)

        # update extremes
        if st.side == "BUY":
            st.highest = max(st.highest, ltp) if st.highest else ltp
        else:
            st.lowest = min(st.lowest, ltp) if st.lowest else ltp

        await self._update_pnl(sess)

        # --- 🧯 STOP LOSS ---
        if st.avg_price > 0:
            if st.side == "BUY":
                sl_price = st.avg_price * (1.0 - s.stop_loss_pct / 100.0)
                if ltp <= sl_price:
                    logger.warning(
                        f"\033[91m❌ SL HIT ▲ [{sym}] LTP={ltp:.2f} SL={sl_price:.2f}\033[0m"
                    )
                    await self._exit_leg(sess, reason="STOP_LOSS")
                    await self._start_next_leg_or_finish(sess)
                    return
            else:
                sl_price = st.avg_price * (1.0 + s.stop_loss_pct / 100.0)
                if ltp >= sl_price:
                    logger.warning(
                        f"\033[91m❌ SL HIT ▼ [{sym}] LTP={ltp:.2f} SL={sl_price:.2f}\033[0m"
                    )
                    await self._exit_leg(sess, reason="STOP_LOSS")
                    await self._start_next_leg_or_finish(sess)
                    return

        # --- ⚡ CIRCUIT TARGET ---
        if circuit and circuit.upper and st.side == "BUY":
            if ltp >= float(circuit.upper) * 0.9995:
                logger.warning(
                    f"\033[92m🎯 UC TARGET ▲ [{sym}] LTP={ltp:.2f} UPPER={circuit.upper}\033[0m"
                )
                sess.stop_after_exit = True
                await self._exit_leg(sess, reason="UPPER_CIRCUIT_TARGET")
                await self._start_next_leg_or_finish(sess)
                return

        if circuit and circuit.lower and st.side == "SELL":
            if ltp <= float(circuit.lower) * 1.0005:
                logger.warning(
                    f"\033[92m🎯 LC TARGET ▼ [{sym}] LTP={ltp:.2f} LOWER={circuit.lower}\033[0m"
                )

                sess.stop_after_exit = True
                await self._exit_leg(sess, reason="LOWER_CIRCUIT_TARGET")
                await self._start_next_leg_or_finish(sess)
                return

        # --- 🔥 TRAILING STOP LOSS ---
        if st.side == "BUY" and st.highest:
            tsl_price = st.highest * (1.0 - s.trailing_sl_pct / 100.0)
            if ltp <= tsl_price:
                logger.warning(
                    f"🛑 TSL HIT  {_color_side_word('BUY')} [{sym}] LTP={ltp:.2f} TSL={tsl_price:.2f} HIGH={st.highest:.2f}"
                )
                await self._exit_leg(sess, reason="TRAILING_SL", tsl_est=tsl_price)
                await self._start_next_leg_or_finish(sess)
                return

        if st.side == "SELL" and st.lowest:
            tsl_price = st.lowest * (1.0 + s.trailing_sl_pct / 100.0)
            if ltp >= tsl_price:
                logger.warning(
                    f"🛑 TSL HIT  {_color_side_word('SELL')} [{sym}] LTP={ltp:.2f} TSL={tsl_price:.2f} LOW={st.lowest:.2f}"
                )
                await self._exit_leg(sess, reason="TRAILING_SL", tsl_est=tsl_price)
                await self._start_next_leg_or_finish(sess)
                return
            
        # --- add ladder step ---
        if st.entries < (1 + s.max_adds_per_leg):
            if st.side == "BUY":
                next_add = st.last_add_price * (1.0 + s.threshold_pct / 100.0) if st.last_add_price else 0.0
                logger.debug(
                    f"🔍 ADD CHECK  {_color_side_word('BUY')} [{sym}] LTP={ltp:.2f} LAST={st.last_add_price:.2f} TH={s.threshold_pct:.2f}% NEXT={next_add:.2f}"
                )
                if next_add and ltp >= next_add:
                    logger.info(
                        f"🚀 ADD EXEC  {_color_side_word('BUY')} [{sym}] LTP={ltp:.2f} NEXT={next_add:.2f}"
                    )
                    await self._add_position(sess, ltp, target_price=next_add)
                    return
            else:
                next_add = st.last_add_price * (1.0 - s.threshold_pct / 100.0) if st.last_add_price else 0.0
                logger.debug(
                    f"🔍 ADD CHECK  {_color_side_word('SELL')} [{sym}] LTP={ltp:.2f} LAST={st.last_add_price:.2f} TH={s.threshold_pct:.2f}% NEXT={next_add:.2f}"
                )
                if next_add and ltp <= next_add:
                    logger.info(
                        f"🚀 ADD EXEC  {_color_side_word('SELL')} [{sym}] LTP={ltp:.2f} NEXT={next_add:.2f}"
                    )
                    await self._add_position(sess, ltp, target_price=next_add)
                    return

    async def _add_position(self, sess: LadderSession, ltp: float, target_price: Optional[float] = None) -> None:
        st = sess.leg_state
        if not st or st.status != "RUNNING":
            return

        lock = await self._lock(sess.symbol, "add", ttl_ms=900)
        if lock != 1:
            await push_event(self.redis, self.user_id, sess.symbol, "ADD_BLOCKED", {"reason": "LOCK_BUSY_OR_KILL"})
            return

        qty_step = self._calc_step_qty(sess.settings, ltp)
        if qty_step <= 0:
            return
        logger.info(
            "➕ ADD PLACE  "
            + f"{_color_side_word(st.side)} "
            + f"[{sess.symbol}]  ADD_QTY={qty_step}  LTP={ltp}"
        )
        try:
            oid = await self._place_mis_market(
                symbol=sess.symbol,
                side=st.side,
                qty=qty_step,
                ltp=ltp,
                order_tag="ADD",
                reason="ADD_STEP",
                leg=sess.leg,
            )
            st.last_add_order_id = str(oid)

            exec_avg = await self._get_executed_avg_price(str(oid))
            fill_price = exec_avg if (exec_avg and exec_avg > 0) else float(ltp)

        except Exception as e:
            logger.error("ADD_ORDER_FAILED user=%s symbol=%s err=%s", self.user_id, sess.symbol, e, exc_info=True)
            if self._is_insufficient(str(e)):
                sess.stop_after_exit = True
                try:
                    # prevent further adds for this session
                    sess.settings.max_adds_per_leg = max(0, int(st.entries) - 1)
                except Exception:
                    pass
            await push_event(self.redis, self.user_id, sess.symbol, "ADD_FAILED", {"err": str(e)})
            # snapshot for UI to show error
            sess.updated_ts = time.time()
            await self._snapshot(sess)
            return
        prev_qty = int(st.qty or 0)
        new_qty = st.qty + qty_step
        if new_qty > 0:
            st.avg_price = ((st.avg_price * st.qty) + (fill_price * qty_step)) / new_qty
        st.qty = new_qty
        st.entries += 1
        st.last_add_price = float(fill_price)
        logger.info(
            "➕ ADD FILL  "
            + f"[{sess.symbol}]  OID={oid}  LTP={ltp:.2f}  FILL={fill_price:.2f}  "
            + f"NEW_AVG={st.avg_price:.2f}  QTY={int(st.qty)}  ENTRIES={int(st.entries)}"
        )
        logger.info(
            "➕ ADD QTY   "
            + f"[{sess.symbol}]  QTY={prev_qty}->{int(st.qty)}  AVG={float(st.avg_price):.2f}"
        )
        if st.side == "BUY":
            st.highest = max(st.highest, ltp)
        else:
            st.lowest = min(st.lowest, ltp)

        hist = self._get_cycle_hist(sess)
        side_key = "B" if st.side == "BUY" else "S"
        if target_price is not None:
            hist[side_key]["est"].append(float(target_price))
        else:
            hist[side_key]["est"].append(float(ltp))
        hist[side_key]["exec"].append(float(fill_price))

        # Add slippage log (expected vs actual)
        try:
            if target_price is not None and float(target_price) > 0:
                slip = float(fill_price) - float(target_price)
                logger.info(
                    "ADD SLIP   %s [%s] EXP=%.2f EXEC=%.2f DIFF=%.2f",
                    _color_side_word(st.side), sess.symbol, float(target_price), float(fill_price), slip
                )
        except Exception:
            pass

        sess.updated_ts = time.time()
        await self._snapshot(sess)

        logger.info(
            "➕ ADD PLACED "
            + f"{_color_side_word(st.side)} "
            + f"[{sess.symbol}]  OID={oid}  NEW_QTY={int(st.qty)}  NEW_AVG={st.avg_price:.2f}  ENTRIES={int(st.entries)}"
        )
        await push_event(self.redis, self.user_id, sess.symbol, "ADD_PLACED",
                         {"side": st.side, "order_id": oid, "add_qty": qty_step, "new_qty": st.qty, "new_avg": st.avg_price, "ltp": ltp})


    async def _update_pnl(self, sess: LadderSession) -> None:
        st = sess.leg_state
        if not st or st.qty <= 0 or st.avg_price <= 0:
            return
        ltp = self.ticks.get(sess.token, 0.0)
        if not ltp:
            return
        if st.side == "BUY":
            st.unrealized_pnl = (ltp - st.avg_price) * st.qty
        else:
            st.unrealized_pnl = (st.avg_price - ltp) * st.qty

    async def _snapshot(self, sess: LadderSession) -> None:
        """
        Write lightweight snapshot to Redis for dashboard polling.
        """
        try:
            # Ensure entry row stays aligned in cycle_hist (C1B1/C1EB1)
            try:
                if sess.leg_state:
                    side_key = "B" if sess.leg_state.side == "BUY" else "S"
                    hist = self._get_cycle_hist(sess)
                    est_list = hist[side_key].get("est") or []
                    exec_list = hist[side_key].get("exec") or []
                    exp = float(sess.leg_state.entry_price_internal or 0.0)
                    exe = float(sess.leg_state.entry_price_exec or 0.0)
                    if exp > 0:
                        if not est_list:
                            est_list.append(exp)
                        else:
                            est_list[0] = exp
                    if exe > 0:
                        if not exec_list:
                            exec_list.append(exe)
                        else:
                            exec_list[0] = exe
            except Exception:
                pass
            sess.updated_ts = time.time()
            payload = json.dumps(sess.to_public_dict())
            await self.redis.set(k_state(sess.user_id, sess.symbol), payload)
            logger.debug("SNAPSHOT user=%s symbol=%s bytes=%s", sess.user_id, sess.symbol, len(payload))
        except Exception as e:
            logger.error("SNAPSHOT_FAILED user=%s symbol=%s err=%s", sess.user_id, sess.symbol, e, exc_info=True)
            await push_event(self.redis, self.user_id, sess.symbol, "SNAPSHOT_FAILED", {"err": str(e)})

    async def remove_session(self, symbol: str) -> None:
        symbol = symbol.strip().upper()
        self.sessions.pop(symbol, None)
        self._unregister_symbol(symbol)
