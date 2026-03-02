# fetch_nse_cash_instruments.py
import json
import sys
import time
import os
import redis
from kiteconnect import KiteConnect
from datetime import datetime
import pytz

OUT_FILE = "nse_eq_instruments.json"
CIRCUIT_TTL_SEC = 24 * 60 * 60
_CIRCUIT_FILE = "circuit_cache.json"
PREV_TTL_SEC = 12 * 60 * 60

# Redis
REDIS_URL = os.getenv("REDIS_URL")
if REDIS_URL:
    r = redis.from_url(REDIS_URL, decode_responses=True)
else:
    r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

def _get_arg(name: str, default=None):
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default

# Optional: python fetch_nse_cash_instruments.py --user-id 1
USER_ID = _get_arg("--user-id", "1")
if not str(USER_ID).isdigit():
    raise RuntimeError("--user-id must be numeric")
USER_ID = str(USER_ID)

def _get_int_arg(name: str, default: int):
    val = _get_arg(name, None)
    if val is None:
        return default
    try:
        return int(val)
    except Exception:
        return default

# 1) Try SYSTEM WS account keys (if you use them)
API_KEY = r.get("zerodha:ws:api_key")
ACCESS_TOKEN = r.get("zerodha:ws:access_token")

# 2) Fallback: use logged-in user keys api_key:<user_id>, access_token:<user_id>
if not API_KEY or not ACCESS_TOKEN:
    API_KEY = r.get(f"api_key:{USER_ID}")
    ACCESS_TOKEN = r.get(f"access_token:{USER_ID}")

if not API_KEY or not ACCESS_TOKEN:
    raise RuntimeError(
        "API_KEY / ACCESS_TOKEN not found.\n"
        "Expected either:\n"
        "  zerodha:ws:api_key + zerodha:ws:access_token\n"
        "OR\n"
        f"  api_key:{USER_ID} + access_token:{USER_ID}"
    )

kite = KiteConnect(api_key=str(API_KEY))
kite.set_access_token(str(ACCESS_TOKEN))

def _now_ist():
    tz = pytz.timezone("Asia/Kolkata")
    return datetime.now(tz)


def fetch_nse_cash():
    print("⬇️ Fetching Zerodha instruments ...")
    data = kite.instruments()  # list[dict]

    symbol_to_token = {}
    token_to_symbol = {}
    tokens = []

    count = 0
    for ins in data:
        if (
            ins.get("exchange") == "NSE"
            and ins.get("segment") == "NSE"
            and ins.get("instrument_type") == "EQ"
            and "-" not in ins.get("tradingsymbol", "")
            and "NAV" not in ins.get("tradingsymbol", "")
        ):
            symbol = ins["tradingsymbol"].upper().strip()
            token = int(ins["instrument_token"])

            symbol_to_token[symbol] = token
            token_to_symbol[str(token)] = symbol
            tokens.append(token)
            count += 1

    payload = {
        "count": count,
        "symbol_to_token": symbol_to_token,
        "token_to_symbol": token_to_symbol,
        "tokens": tokens,
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"✅ NSE CASH instruments saved: {count} -> {OUT_FILE}")
    return payload


def fetch_circuit_prev(symbols):
    if not symbols:
        print("No symbols to fetch circuit/prev.")
        return
    kite = KiteConnect(api_key=str(API_KEY))
    kite.set_access_token(str(ACCESS_TOKEN))

    day = _now_ist().strftime("%Y%m%d")
    fetched = 0
    CHUNK = 50

    circuit_cache = {}

    for i in range(0, len(symbols), CHUNK):
        batch = symbols[i:i + CHUNK]
        instruments = [f"NSE:{s}" for s in batch]
        try:
            q = kite.quote(instruments)
        except Exception as e:
            print(f"Quote failed for batch {i // CHUNK + 1}: {e}")
            continue

        for k, v in (q or {}).items():
            sym = str(k).split(":")[-1].upper()
            upper = v.get("upper_circuit_limit")
            lower = v.get("lower_circuit_limit")
            ohlc = v.get("ohlc") or {}
            prev_close = ohlc.get("close")

            payload = {
                "upper": float(upper) if upper is not None else None,
                "lower": float(lower) if lower is not None else None,
                "ts": time.time()
            }
            r.setex(f"circuit:{sym}", CIRCUIT_TTL_SEC, json.dumps(payload))
            circuit_cache[sym] = payload

            if prev_close not in (None, ""):
                r.set(f"prev:{day}:{sym}", str(float(prev_close)), ex=PREV_TTL_SEC)

            fetched += 1

        time.sleep(0.2)

    if circuit_cache:
        try:
            with open(_CIRCUIT_FILE, "w", encoding="utf-8") as f:
                json.dump(circuit_cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    print(f"✅ Circuit/Prev cached for {fetched} symbols")

if __name__ == "__main__":
    fetch_circuit_flag = "--fetch-circuit" in sys.argv
    if "--watch" in sys.argv:
        print("Watch mode removed. Use one-time fetch with --fetch-circuit.")
        sys.exit(0)

    data = fetch_nse_cash()
    if fetch_circuit_flag:
        symbols = sorted(list((data.get("symbol_to_token") or {}).keys()))
        fetch_circuit_prev(symbols)
