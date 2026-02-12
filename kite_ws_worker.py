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
from dotenv import load_dotenv
import os



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

class EnsureUserFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "user"):
            record.user = USER_ID  # ensures 3rd-party logs don't crash formatter
        return True

# add filter to all handlers (root)
root = logging.getLogger()
for h in root.handlers:
    h.addFilter(EnsureUserFilter())

log = logging.LoggerAdapter(logging.getLogger("KITE_WS"), {"user": USER_ID})


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("❌ REDIS_URL missing in .env")

r = redis.from_url(
    REDIS_URL,
    decode_responses=True
)

try:
    r.ping()
    print("✅ Redis connected (worker)")
except Exception as e:
    print("❌ Redis auth failed (worker):", e)
    sys.exit(1)

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
ORDER_BUFFER = []  # ✅ New buffer for order updates
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
            del TICK_BUFFER[:1000]

def push_order_update(payload: dict):
    """Thread-safe append for order updates (Immediate high priority)."""
    with BUF_LOCK:
        ORDER_BUFFER.append(payload)

async def ws_sender_loop():
    """
    Persistent reconnecting sender loop.
    Reads tick batches AND order updates.
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
                open_timeout=10,
                close_timeout=3,
                max_size=5_000_000
            ) as ws:
                log.info("✅ Connected to FastAPI ingest WS")

                while True:
                    if r.get(f"kill:{USER_ID}"):
                        return

                    # 1️⃣ Flush Order Updates (Priority)
                    order_batch = None
                    with BUF_LOCK:
                        if ORDER_BUFFER:
                            order_batch = list(ORDER_BUFFER)
                            ORDER_BUFFER.clear()
                    
                    if order_batch:
                        for update in order_batch:
                            await ws.send(json.dumps({"type": "order_update", "payload": update}))
                    
                    # 2️⃣ Flush Ticks
                    tick_batch = None
                    with BUF_LOCK:
                        if TICK_BUFFER:
                            tick_batch = TICK_BUFFER[:BATCH_SIZE]
                            del TICK_BUFFER[:BATCH_SIZE]

                    if tick_batch:
                        await ws.send(json.dumps({"type": "ticks", "ticks": tick_batch}))
                    
                    if not order_batch and not tick_batch:
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
    time.sleep(1.0)


def on_error(ws, code, reason):
    log.error(f"❌ Kite WS error | {code} | {reason}")

def on_order_update(ws, data):
    """
    Called when order status changes (REJECTED, OPEN, COMPLETE).
    """
    try:
        # log.info(f"Order Update: {data}")
        push_order_update(data)
    except Exception as e:
        log.error(f"Error in on_order_update: {e}")


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
            prev = float(ohlc.get("close") or ltp)
            if prev <= 0:
                prev = ltp

            # ✅ symbol also included (IMPORTANT for UI updates)
            symbol = (t.get("tradingsymbol") or "").strip().upper()

            # ✅ prefer fast TBQ/TSQ fields (MODE_FULL)
            tbq = int(t.get("buy_quantity") or 0)
            tsq = int(t.get("sell_quantity") or 0)

            # ✅ volume traded (MODE_FULL)
            volume = int(
                t.get("volume_traded")
                or t.get("volume")
                or t.get("volume_traded_today")
                or 0
            )

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
                "volume": volume,
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

    # Create a persistent session to reuse TCP connections (Avoid WinError 10048)
    sess = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    sess.mount("http://", adapter)

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
                time.sleep(1.0)
                continue

            # Re-use session
            resp = sess.get(
                FASTAPI_HTTP_TOKENS,
                params={"user_id": int(USER_ID)},
                timeout=5.0 
            )
            
            if resp.status_code != 200:
                time.sleep(1.0)
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

            # Reduce polling frequency to save CPU/Sockets
            time.sleep(1.0)

        except Exception as e:
            log.error(f"Token sync error: {e}")
            time.sleep(5.0) # Backoff on error




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
    kws.on_order_update = on_order_update  # ✅ ADDED CALLBACK

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
