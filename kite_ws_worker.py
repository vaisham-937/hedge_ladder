# kite_ws_worker.py
import sys
import time
import json
import threading
import logging
import redis
from kiteconnect import KiteTicker

import asyncio
import websockets
import requests


# -------------------------------------------------
# ARGUMENT
# -------------------------------------------------
if "--user-id" not in sys.argv:
    raise RuntimeError("Usage: python kite_ws_worker.py --user-id <USER_ID>")

USER_ID = sys.argv[sys.argv.index("--user-id") + 1]
if not str(USER_ID).isdigit():
    raise RuntimeError("--user-id must be numeric")

USER_ID = str(USER_ID)

FASTAPI_WS_INGEST = f"ws://127.0.0.1:8000/ws/ingest?user_id={USER_ID}"
FASTAPI_HTTP_TOKENS = "http://127.0.0.1:8000/api/ws-tokens"


# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | WS | USER=%(user)s | %(message)s"
)
log = logging.LoggerAdapter(
    logging.getLogger("KITE_WS"),
    {"user": USER_ID}
)


# -------------------------------------------------
# REDIS (DB0 ONLY)
# -------------------------------------------------
r = redis.Redis(
    host="127.0.0.1",
    port=6379,
    db=0,
    decode_responses=True
)


# -------------------------------------------------
# LOAD TOKEN (FASTAPI STYLE)
# -------------------------------------------------
API_KEY = r.get(f"api_key:{USER_ID}")
ACCESS_TOKEN = r.get(f"access_token:{USER_ID}")

if not API_KEY or not ACCESS_TOKEN:
    raise RuntimeError(
        f"❌ Redis missing token. Expected keys:\n"
        f"api_key:{USER_ID}\n"
        f"access_token:{USER_ID}"
    )

log.info("🔐 Zerodha token loaded from Redis")


# -------------------------------------------------
# KITE WS
# -------------------------------------------------
kws = KiteTicker(API_KEY, ACCESS_TOKEN)

SUBSCRIBED = set()
LOCK = threading.Lock()

WS_TOKENS_KEY = f"ws:{USER_ID}:tokens"
WS_CONNECTED = False


# -------------------------------------------------
# GLOBAL BUFFER + SENDER THREAD
# -------------------------------------------------
TICK_BUFFER = []
BUF_LOCK = threading.Lock()

MAX_BUF = 8000          # hard upper bound
BATCH_SIZE = 300        # per ws send
IDLE_SLEEP = 0.02       # 20ms when no ticks
RECONNECT_SLEEP = 1.0   # seconds


def push_tick(payload: dict):
    """Thread-safe append to in-memory buffer."""
    with BUF_LOCK:
        TICK_BUFFER.append(payload)
        if len(TICK_BUFFER) > MAX_BUF:
            # drop oldest to avoid memory blow
            del TICK_BUFFER[:1000]


async def ws_sender_loop():
    """
    Persistent reconnecting sender loop.
    Reads tick batches from TICK_BUFFER and sends to FastAPI /ws/ingest.
    """
    while True:
        if r.get(f"kill:{USER_ID}"):
            log.warning("🛑 Kill switch active. Sender loop exiting.")
            return

        try:
            async with websockets.connect(
                FASTAPI_WS_INGEST,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=10,          # ✅ add
                close_timeout=3,          # ✅ add
                max_size=5_000_000        # ✅ little higher
            ) as ws:
                log.info("✅ Connected to FastAPI ingest WS")

                while True:
                    if r.get(f"kill:{USER_ID}"):
                        log.warning("🛑 Kill switch active. Stop sending ticks.")
                        return

                    batch = None
                    with BUF_LOCK:
                        if TICK_BUFFER:
                            batch = TICK_BUFFER[:BATCH_SIZE]
                            del TICK_BUFFER[:BATCH_SIZE]

                    if batch:
                        await ws.send(json.dumps({"type": "ticks", "ticks": batch}))
                    else:
                        await asyncio.sleep(IDLE_SLEEP)

        except Exception as e:
            log.error(f"❌ ingest ws reconnecting... {e}")
            await asyncio.sleep(RECONNECT_SLEEP)


def start_sender_thread():
    def _run():
        asyncio.run(ws_sender_loop())
    threading.Thread(target=_run, daemon=True, name=f"sender_user_{USER_ID}").start()


# -------------------------------------------------
# CALLBACKS
# -------------------------------------------------
def on_connect(ws, response):
    global WS_CONNECTED
    WS_CONNECTED = True
    log.info("✅ Kite WS connected")


def on_close(ws, code, reason):
    global WS_CONNECTED
    WS_CONNECTED = False
    log.warning(f"❌ Kite WS closed | {code} | {reason}")


def on_error(ws, code, reason):
    log.error(f"❌ Kite WS error | {code} | {reason}")


def _sum_depth_qty(depth: dict, side: str) -> int:
    try:
        arr = (depth or {}).get(side) or []
        return int(sum(int(o.get("quantity", 0) or 0) for o in arr))
    except Exception:
        return 0

def on_ticks(ws, ticks):
    """
    Receives Kite ticks (threaded=True runs in background thread).
    We convert to compact payload and push into buffer.
    """
    try:
        now_ts = int(time.time())

        for t in ticks:
            token = t.get("instrument_token")
            if not token:
                continue
            ltp = float(t.get("last_price") or 0.0)
            if ltp <= 0:
                continue

            ohlc = t.get("ohlc") or {}
            high = float(ohlc.get("high") or ltp)
            low = float(ohlc.get("low") or ltp)

            # Your dashboard logic expects "prev" baseline
            prev = float(ohlc.get("open") or ohlc.get("close") or ltp)
            if prev <= 0:
                prev = ltp

            # ✅ symbol also included (IMPORTANT for UI updates)
            symbol = (t.get("tradingsymbol") or "").strip().upper()

            # ✅ prefer fast TBQ/TSQ fields (MODE_FULL)
            tbq = int(t.get("buy_quantity") or 0)
            tsq = int(t.get("sell_quantity") or 0)

            # fallback to depth sum if needed
            if tbq == 0 or tsq == 0:
                depth = t.get("depth") or {}
                if tbq == 0:
                    tbq = _sum_depth_qty(depth, "buy")
                if tsq == 0:
                    tsq = _sum_depth_qty(depth, "sell")

            payload = {
                "token": int(token),
                "symbol": symbol,   # ✅ VERY IMPORTANT
                "ltp": ltp,
                "high": high,
                "low": low,
                "prev": prev,
                "tbq": tbq,
                "tsq": tsq,
                "ts": now_ts
            }
            push_tick(payload)

    except Exception:
        log.error("❌ on_ticks crashed", exc_info=True)


# -------------------------------------------------
# TOKEN SUBSCRIBE LOOP
# -------------------------------------------------
def sync_tokens():
    """
    Reads tokens from FastAPI (RAM) instead of Redis.
    """
    last_seen = set()

    while True:
        try:
            if r.get(f"kill:{USER_ID}"):
                log.warning("🛑 Kill switch active. Closing Kite WS...")
                try:
                    kws.close()
                except:
                    pass
                return

            if not WS_CONNECTED:
                time.sleep(0.2)
                continue

            resp = requests.get(
                FASTAPI_HTTP_TOKENS,
                params={"user_id": int(USER_ID)},
                timeout=1.5
            )
            if resp.status_code != 200:
                time.sleep(0.3)
                continue

            data = resp.json()
            tokens = set(int(x) for x in (data.get("tokens") or []) if str(x).isdigit())

            new = tokens - last_seen
            if new:
                with LOCK:
                    kws.subscribe(list(new))
                    kws.set_mode(kws.MODE_FULL, list(new))
                    SUBSCRIBED.update(new)
                    last_seen = tokens.copy()

                log.info(f"⚡ Instant subscribe: {list(new)}")

            time.sleep(0.2)

        except Exception as e:
            log.error(f"Token sync error: {e}", exc_info=True)
            time.sleep(1)




# -------------------------------------------------
# START
# -------------------------------------------------
def start():
    log.info("🚀 Starting Kite WS Worker")
    start_sender_thread()

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error

    threading.Thread(target=sync_tokens, daemon=True, name=f"token_sync_user_{USER_ID}").start()
    kws.connect(threaded=True)


# -------------------------------------------------
if __name__ == "__main__":
    start()
    while True:
        if r.get(f"kill:{USER_ID}"):
            log.warning("🛑 Kill switch active. Exiting ws worker main loop.")
            break
        time.sleep(2)


