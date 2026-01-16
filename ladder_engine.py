
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
import logging
import math
import os
import sys
import time, datetime
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, Optional, Literal, List, Tuple

try:
    import redis.asyncio as aioredis  # redis-py >= 4.2
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore

from kiteconnect import KiteConnect


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


def setup_logger() -> None:
    level = _env_log_level()
    logger.setLevel(level)

    # Avoid duplicate handlers (especially with reload)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        h = logging.StreamHandler(sys.stdout)
        h.setLevel(level)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
        h.setFormatter(fmt)
        logger.addHandler(h)

    # Keep clean (don’t spam root logger)
    logger.propagate = False
setup_logger()


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
    threshold_pct: float = 1.0       # add threshold per step (%)
    stop_loss_pct: float = 1.0            # immediate SL from avg (%)
    trailing_sl_pct: float = 1.0          # trailing from peak/low (%)
    ladder_cycles: int = 1                # 1 cycle = 1 buy ladder + 1 sell ladder
    max_adds_per_leg: int = 2             # safety cap
    slippage_bps: float = 0.0             # optional, not used for MIS market in this version

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
            "ltp": None,  
            "leg_state": asdict(self.leg_state) if self.leg_state else None,
            "settings": asdict(self.settings),
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
        self.sessions: Dict[str, LadderSession] = {}  # symbol -> session
        self.circuits: Dict[str, Circuit] = {}        # symbol -> Circuit

        self.SYMBOL_TOKEN_RAM = None
        self.order_worker = OrderWorker()
        self._kill_handled = False


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

        logger.info("✅ LadderEngine started for user_id=%s", self.user_id)
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
            logger.info("SETTINGS saved STOCK user=%s symbol=%s settings=%s", self.user_id, symbol, payload)
        else:
            await self.redis.set(k_settings_universal(self.user_id), payload)
            logger.info("SETTINGS saved UNIVERSAL user=%s settings=%s", self.user_id, payload)

        await push_event(self.redis, self.user_id, symbol, "SETTINGS_SAVED", {"mode": mode, "settings": asdict(settings)})

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

        def _fetch_quote(api_key: str, access_token: str, sym: str) -> Dict[str, Any]:
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            q = kite.quote([f"NSE:{sym}"])
            return q.get(f"NSE:{sym}", {}) if isinstance(q, dict) else {}

        try:
            q = await asyncio.to_thread(_fetch_quote, self.api_key, self.access_token, symbol)
        except Exception as e:
            logger.error("CIRCUIT fetch failed symbol=%s err=%s", symbol, e, exc_info=True)
            await push_event(self.redis, self.user_id, symbol, "CIRCUIT_FETCH_FAIL", {"err": str(e)})

            # 🔥 CRITICAL FIX — NEVER BLOCK TRADING
            # If circuit fetch fails, just return empty circuit
            return Circuit()

        upper = q.get("upper_circuit_limit")
        lower = q.get("lower_circuit_limit")

        c = Circuit(
            upper=float(upper) if upper is not None else None,
            lower=float(lower) if lower is not None else None,
            ts=time.time(),
        )
        self.circuits[symbol] = c
        await self.redis.setex(k_circuit(symbol), 60 * 30, json.dumps(asdict(c)))

        logger.info("CIRCUIT fetched symbol=%s upper=%s lower=%s", symbol, c.upper, c.lower)
        await push_event(self.redis, self.user_id, symbol, "CIRCUIT_FETCHED", {"upper": c.upper, "lower": c.lower})
        return c

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

        # 1) Already running?
        existing = self.sessions.get(symbol)
        if existing and existing.active:
            logger.info("START_LADDER already_running user=%s symbol=%s", self.user_id, symbol)
            return {"status": "already_running", "session": existing.to_public_dict()}

        # 2) Ensure RAM map exists
        if self.SYMBOL_TOKEN_RAM is None:
            self.SYMBOL_TOKEN_RAM = {}

        token_raw = self.SYMBOL_TOKEN_RAM.get(self.user_id, {}).get(symbol)
        logger.info(
            "START_LADDER user=%s symbol=%s side=%s mode=%s token_in_ram=%s payload=%s",
            self.user_id, symbol, started_side, settings_mode, token_raw, settings_payload or {}
        )
        await push_event(self.redis, self.user_id, symbol, "START_LADDER", {
            "side": started_side,
            "mode": settings_mode,
            "token_in_ram": token_raw,
            "settings_payload": settings_payload or {}
        })

        # 3) Load + merge settings
        settings = await self.load_settings(settings_mode, symbol)
        if settings_payload:
            settings = LadderSettings.from_dict({**asdict(settings), **settings_payload})
            await self.save_settings(settings_mode, symbol, settings)

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
            )
            self.sessions[symbol] = sess
            await self._snapshot(sess)

            logger.warning(
            f"\033[93m⏳ WAITING FOR FEED\033[0m  "
            f"\033[1m[{symbol}]\033[0m  "
            f"USER={self.user_id}"
        )
            await push_event(self.redis, self.user_id, symbol, "WAIT_FEED", {})
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
        )
        self.sessions[symbol] = sess
        await self._snapshot(sess)

        # 6) If LTP already present -> enter instantly
        ltp = float(self.ticks.get(token, 0.0) or 0.0)
        logger.info(
    "\033[92m⚡ INSTANT ENTRY\033[0m   "
    + ("\033[92m▲ BUY " if started_side == "BUY" else "\033[91m▼ SELL ")
    + f"\033[1m[{symbol}]\033[0m  "
    + f"LTP={ltp:.2f}"
)

        if ltp > 0:
            await self._enter_leg(sess)
            await self._snapshot(sess)
            return {"status": "started", "session": sess.to_public_dict()}

        # else wait for first tick
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


    # ------------ internal: leg entry/exit ------------
    def _calc_step_qty(self, settings: LadderSettings, ltp: float) -> int:
        if ltp <= 0:
            return 0
        if settings.qty_mode == "QTY":
            return int(max(1, settings.per_trade_qty))
        return int(max(1, math.floor(settings.per_trade_capital / ltp)))

    async def _enter_leg(self, sess: LadderSession) -> None:
        if not sess.active:
            return
        symbol = sess.symbol
        side = sess.leg
        s = sess.settings
        ltp = self.ticks.get(sess.token, 0.0)
        logger.info("ENTER_LEG user=%s symbol=%s side=%s token=%s ltp=%s",
                    self.user_id, symbol, side, sess.token, ltp)
        await push_event(self.redis, self.user_id, symbol, "ENTER_LEG", {"side": side, "token": sess.token, "ltp": ltp})

        if not ltp or ltp <= 0:
            sess.leg_state.status = "ERROR"
            sess.leg_state.reason = "NO_LTP"
            logger.error("NO_LTP user=%s symbol=%s token=%s", self.user_id, symbol, sess.token)
            await push_event(self.redis, self.user_id, symbol, "ERROR_NO_LTP", {"token": sess.token})
            await self._snapshot(sess)
            return

        qty = self._calc_step_qty(s, ltp)
        if qty <= 0:
            sess.leg_state.status = "ERROR"
            sess.leg_state.reason = "BAD_QTY"
            logger.error("BAD_QTY user=%s symbol=%s ltp=%s settings=%s", self.user_id, symbol, ltp, asdict(s))
            await push_event(self.redis, self.user_id, symbol, "ERROR_BAD_QTY", {"ltp": ltp, "settings": asdict(s)})
            await self._snapshot(sess)
            return

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

        oid = await self._place_mis_market(symbol=symbol, side=side, qty=qty)

        exec_avg = await self._get_executed_avg_price(str(oid))
        base_price = exec_avg if (exec_avg and exec_avg > 0) else float(ltp)
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

        sess.leg_state.reason = ""

        logger.info("ENTRY_PLACED user=%s symbol=%s side=%s qty=%s ltp=%s oid=%s",
                    self.user_id, symbol, side, qty, ltp, oid)
        await push_event(self.redis, self.user_id, symbol, "ENTRY_PLACED",
                         {"side": side, "qty": qty, "ltp": ltp, "order_id": oid})

        await self._snapshot(sess)

        asyncio.create_task(self.get_circuit(symbol), name=f"circuit_prefetch:{self.user_id}:{symbol}")


    async def _exit_leg(self, sess: LadderSession, reason: str) -> None:
        symbol = sess.symbol
        st = sess.leg_state
        if not st or st.status not in ("RUNNING", "EXITING"):
            return

        if st.qty <= 0:
            st.status = "DONE"
            st.reason = reason
            logger.info("EXIT_SKIP qty=0 user=%s symbol=%s reason=%s", self.user_id, symbol, reason)
            await push_event(self.redis, self.user_id, symbol, "EXIT_SKIP_QTY0", {"reason": reason})
            return

        lock = await self._lock(symbol, "exit", ttl_ms=1500)
        if lock != 1:
            st.reason = "KILLED" if lock == -2 else "EXIT_LOCKED"
            st.status = "ERROR" if lock == -2 else st.status
            logger.warning("EXIT_BLOCKED user=%s symbol=%s reason=%s", self.user_id, symbol, st.reason)
            await push_event(self.redis, self.user_id, symbol, "EXIT_BLOCKED", {"reason": st.reason})
            await self._snapshot(sess)
            return

        logger.warning("EXIT_TRIGGER user=%s symbol=%s reason=%s side=%s qty=%s avg=%s",
                       self.user_id, symbol, reason, st.side, st.qty, st.avg_price)
        await push_event(self.redis, self.user_id, symbol, "EXIT_TRIGGER",
                         {"reason": reason, "side": st.side, "qty": st.qty, "avg": st.avg_price})

        st.status = "EXITING"
        st.reason = reason
        await self._snapshot(sess)

        exit_side: Side = "SELL" if st.side == "BUY" else "BUY"
        qty = int(st.qty)
        oid = await self._place_mis_market(symbol=symbol, side=exit_side, qty=qty)

        logger.info("EXIT_PLACED user=%s symbol=%s exit_side=%s qty=%s oid=%s",
                    self.user_id, symbol, exit_side, qty, oid)
        await push_event(self.redis, self.user_id, symbol, "EXIT_PLACED",
                         {"exit_side": exit_side, "qty": qty, "order_id": oid})

        st.status = "DONE"
        st.qty = 0
        await self._snapshot(sess)

        logger.info("LEG_DONE user=%s symbol=%s reason=%s", self.user_id, symbol, reason)
        await push_event(self.redis, self.user_id, symbol, "LEG_DONE", {"reason": reason})

    
    async def _place_mis_market(self, symbol: str, side: Side, qty: int) -> str:
        """Place MIS market order via Kite in the order worker."""
        def _place(api_key: str, access_token: str, sym: str, s: Side, q: int) -> str:
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
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
            logger.info(
    "\033[94m📤 ORDER SENT\033[0m   "
    + ("\033[92m▲ BUY " if side == "BUY" else "\033[91m▼ SELL ")
    + f"\033[1m[{symbol}]\033[0m  "
    + f"QTY={qty}  USER={self.user_id}  "
    + f"TIME={datetime.now().strftime('%H:%M:%S')}"
)
            t0 = time.perf_counter()
            oid = await self.order_worker.submit(
                _place,
                api_key=self.api_key,
                access_token=self.access_token,
                sym=symbol,
                s=side,
                q=qty
            )
            latency = (time.perf_counter() - t0) * 1000
            logger.info(
            "\033[92m✅ ORDER PLACED\033[0m  "
            + ("\033[92m▲ BUY " if side == "BUY" else "\033[91m▼ SELL ")
            + f"\033[1m[{symbol}]\033[0m  "
            + f"QTY={qty}  "
            + f"OID={oid}  "
            + f"LATENCY={latency:.1f}ms  "
            + f"TIME={datetime.now().strftime('%H:%M:%S')}"
        )
            return str(oid)
        except Exception as e:
            logger.error(
            "\033[91m❌ ORDER FAILED\033[0m  "
            + ("\033[92m▲ BUY " if side == "BUY" else "\033[91m▼ SELL ")
            + f"\033[1m[{symbol}]\033[0m  "
            + f"QTY={qty}  ERR={e}  "
            + f"TIME={datetime.now().strftime('%H:%M:%S')}",
            exc_info=True
        )

            await push_event(self.redis, self.user_id, symbol, "ORDER_FAILED",
                             {"side": side, "qty": qty, "err": str(e)})
            raise

    async def ingest_tick(self, token: int, ltp: float) -> None:
        """Called by FastAPI (in-memory). NO Redis here."""
        if not self._running:
            return
        try:
            token = int(token)
            ltp = float(ltp)
            if ltp <= 0:
                return

            self.ticks[token] = ltp

            # evaluate only sessions that match this token
            await self._eval_token(token)

            # if any session is waiting for LTP, try to enter
            for sess in list(self.sessions.values()):
                if not sess.active:
                    continue
                # 🔥 Assign token if waiting for feed
                if sess.token == 0:
                    t = self.SYMBOL_TOKEN_RAM.get(self.user_id, {}).get(sess.symbol)
                    if t:
                        sess.token = int(t)
                        logger.info("FEED_READY user=%s symbol=%s token=%s", self.user_id, sess.symbol, sess.token)

                # 🔥 If this tick matches session, start ladder
                if sess.token == token and sess.leg_state and sess.leg_state.status == "IDLE":
                    if sess.leg_state.reason in ("WAIT_LTP", "WAIT_FEED"):
                        await self._enter_leg(sess)
                        await self._snapshot(sess)


        except Exception:
            # never crash hot path
            return


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
        for sess in self.sessions.values():
            if sess.token == token and sess.active:
                await self._eval_session(sess)

    async def _start_next_leg_or_finish(self, sess: LadderSession) -> None:
        """ After a leg completes, either start the opposite leg (same cycle), or advance cycle, or finish session."""
        if not sess.active:
            return

        # If we just finished BUY, go to SELL; if finished SELL, cycle completed.
        if sess.leg == "BUY":
            sess.leg = "SELL"
        else:
            sess.cycle_index += 1
            if sess.cycle_index >= sess.settings.ladder_cycles:
                sess.active = False
                sess.updated_ts = time.time()
                await self._snapshot(sess)
                logger.info("SESSION_DONE user=%s symbol=%s cycles=%s",
                            self.user_id, sess.symbol, sess.settings.ladder_cycles)
                await push_event(self.redis, self.user_id, sess.symbol, "SESSION_DONE", {})
                return
            sess.leg = "BUY"

        # Prepare next leg as IDLE waiting for LTP
        sess.leg_state = LegState(side=sess.leg, status="IDLE", reason="WAIT_LTP")
        sess.updated_ts = time.time()
        await self._snapshot(sess)

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
                f"\033[91m❌ STOP LOSS HIT ▲  [{sym}]  "
                f"LTP={ltp:.2f}  SL={sl_price:.2f}\033[0m"
            )
                    await self._exit_leg(sess, reason="STOP_LOSS")
                    await self._start_next_leg_or_finish(sess)
                    return
            else:
                sl_price = st.avg_price * (1.0 + s.stop_loss_pct / 100.0)
                if ltp >= sl_price:
                    logger.warning(
                    f"\033[91m❌ STOP LOSS HIT ▼  [{sym}]  "
                    f"LTP={ltp:.2f}  SL={sl_price:.2f}\033[0m"
                )
                    await self._exit_leg(sess, reason="STOP_LOSS")
                    await self._start_next_leg_or_finish(sess)
                    return

        # --- ⚡ CIRCUIT TARGET ---
        if circuit and circuit.upper and st.side == "BUY":
            if ltp >= float(circuit.upper) * 0.9995:
                logger.warning(
                f"\033[92m🎯 UPPER CIRCUIT TARGET  ▲  [{sym}]  "
                f"LTP={ltp:.2f}  UPPER={circuit.upper}\033[0m"
            )
                await self._exit_leg(sess, reason="UPPER_CIRCUIT_TARGET")
                await self._start_next_leg_or_finish(sess)
                return

        if circuit and circuit.lower and st.side == "SELL":
            if ltp <= float(circuit.lower) * 1.0005:
                logger.warning(
                f"\033[92m🎯 LOWER CIRCUIT TARGET ▼  [{sym}]  "
                f"LTP={ltp:.2f}  LOWER={circuit.lower}\033[0m"
            )

                await self._exit_leg(sess, reason="LOWER_CIRCUIT_TARGET")
                await self._start_next_leg_or_finish(sess)
                return

        # --- 🔥 TRAILING STOP LOSS ---
        if st.side == "BUY" and st.highest:
            tsl_price = st.highest * (1.0 - s.trailing_sl_pct / 100.0)
            if ltp <= tsl_price:
                logger.warning(
                f"\033[91m🛑 TRAILING STOP LOSS HIT ▲  [{sym}]  "
                f"LTP={ltp:.2f}  TSL={tsl_price:.2f}  HIGH={st.highest:.2f}\033[0m"
            )
                await self._exit_leg(sess, reason="TRAILING_SL")
                await self._start_next_leg_or_finish(sess)
                return

        if st.side == "SELL" and st.lowest:
            tsl_price = st.lowest * (1.0 + s.trailing_sl_pct / 100.0)
            if ltp >= tsl_price:
                logger.warning(
                f"\033[91m🛑 TRAILING STOP LOSS HIT ▼  [{sym}]  "
                f"LTP={ltp:.2f}  TSL={tsl_price:.2f}  LOW={st.lowest:.2f}\033[0m"
            )
                await self._exit_leg(sess, reason="TRAILING_SL")
                await self._start_next_leg_or_finish(sess)
                return
            
        # --- add ladder step ---
        if st.entries < (1 + s.max_adds_per_leg):
            if st.side == "BUY":
                next_add = st.last_add_price * (1.0 + s.threshold_pct / 100.0) if st.last_add_price else 0.0
                logger.info(
                "\033[90m────────────────────────────────────────\033[0m\n"
                + "\033[96m🔍 ADD CHECK\033[0m   "
                + ("\033[92m▲ BUY " if st.side == "BUY" else "\033[91m▼ SELL ")
                + f"\033[1m[{sym}]\033[0m\n"
                + f"   LTP       : {ltp:.2f}\n"
                + f"   LAST ADD  : {st.last_add_price:.2f}\n"
                + f"   THRESHOLD : {s.threshold_pct:.2f}%\n"
                + f"   NEXT ADD  : \033[93m{next_add:.2f}\033[0m\n"
            )
                if next_add and ltp >= next_add:
                    logger.info(
                ("\033[92m🚀 ADD EXECUTED ▲ " if st.side == "BUY" else "\033[91m🚀 ADD EXECUTED ▼ ")
                + f"\033[1m[{sym}]\033[0m  "
                + f"LTP={ltp:.2f}  ➜  NEXT={next_add:.2f}\033[0m"
            )
                    await self._add_position(sess, ltp)
                    return
            else:
                next_add = st.last_add_price * (1.0 - s.threshold_pct / 100.0) if st.last_add_price else 0.0
                logger.info(
                "\033[90m────────────────────────────────────────\033[0m\n"
                f"\033[96m🔍 ADD CHECK\033[0m   \033[91m▼ SELL\033[0m  "
                f"\033[1m[{sym}]\033[0m\n"
                f"   LTP       : {ltp:.2f}\n"
                f"   LAST ADD  : {st.last_add_price:.2f}\n"
                f"   THRESHOLD : {s.threshold_pct:.2f}%\n"
                f"   NEXT ADD  : \033[93m{next_add:.2f}\033[0m\n"
            )
                if next_add and ltp <= next_add:
                    logger.info(
                    f"\033[91m🚀 ADD EXECUTED ▼  [{sym}]  "
                    f"LTP={ltp:.2f}  ➜  NEXT={next_add:.2f}\033[0m"
                )
                    await self._add_position(sess, ltp)
                    return

    async def _add_position(self, sess: LadderSession, ltp: float) -> None:
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
        logger.info("ADD_PLACE user=%s symbol=%s side=%s add_qty=%s ltp=%s",
                    self.user_id, sess.symbol, st.side, qty_step, ltp)
        try:
            oid = await self._place_mis_market(symbol=sess.symbol, side=st.side, qty=qty_step)

            exec_avg = await self._get_executed_avg_price(str(oid))
            fill_price = exec_avg if (exec_avg and exec_avg > 0) else float(ltp)

        except Exception as e:
            logger.error("ADD_ORDER_FAILED user=%s symbol=%s err=%s", self.user_id, sess.symbol, e, exc_info=True)
            await push_event(self.redis, self.user_id, sess.symbol, "ADD_FAILED", {"err": str(e)})
            # snapshot for UI to show error
            sess.updated_ts = time.time()
            await self._snapshot(sess)
            return
        new_qty = st.qty + qty_step
        if new_qty > 0:
            st.avg_price = ((st.avg_price * st.qty) + (fill_price * qty_step)) / new_qty
        st.qty = new_qty
        st.entries += 1
        st.last_add_price = float(fill_price)
        logger.info(
            "ADD_FILL user=%s symbol=%s oid=%s ltp=%.2f exec_avg=%s fill=%.2f new_avg=%.2f qty=%s entries=%s",
            self.user_id, sess.symbol, oid, float(ltp), str(exec_avg), float(fill_price),
            float(st.avg_price), int(st.qty), int(st.entries)
        )
        if st.side == "BUY":
            st.highest = max(st.highest, ltp)
        else:
            st.lowest = min(st.lowest, ltp)

        sess.updated_ts = time.time()
        await self._snapshot(sess)

        logger.info("ADD_PLACED user=%s symbol=%s side=%s oid=%s new_qty=%s new_avg=%s entries=%s",
                    self.user_id, sess.symbol, st.side, oid, st.qty, st.avg_price, st.entries)
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
