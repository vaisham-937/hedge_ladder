# from kiteconnect import KiteConnect
# import redis


# # Redis
# r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

# # SYSTEM (WS) ACCOUNT
# API_KEY = r.get("zerodha:ws:api_key")
# ACCESS_TOKEN = r.get("zerodha:ws:access_token")

# kite = KiteConnect(api_key=API_KEY)
# kite.set_access_token(ACCESS_TOKEN)


# def fetch_nse_cash():
#     print("⬇️ Fetching Zerodha instruments .....")

#     data = kite.instruments()  # list of dicts

#     count = 0
#     for ins in data:

#         # ✅ NSE CASH EQUITY ONLY
#         if (
#             ins.get("exchange") == "NSE"
#             and ins.get("segment") == "NSE"
#             and ins.get("instrument_type") == "EQ"
#             and "-" not in ins["tradingsymbol"] 
#             and "NAV" not in ins["tradingsymbol"]
#                 # SG / Bonds exclude
#         ):
#             symbol = ins["tradingsymbol"]
#             token = ins["instrument_token"]

#             # Redis mapping
#             r.set(f"symbol_token:{symbol}", token)
#             r.set(f"token_symbol:{token}", symbol) 
#             # 🔥 ADD THIS (MOST IMPORTANT)
#             r.sadd("fetched:tokens", token)

#             count += 1

#     print(f"✅ NSE CASH stocks stored in Redis: {count}")

# if __name__ == "__main__":
#     fetch_nse_cash()

# fetch_nse_cash_instruments.py
import json
import sys
import redis
from kiteconnect import KiteConnect

OUT_FILE = "nse_eq_instruments.json"

# Redis
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

if __name__ == "__main__":
    fetch_nse_cash()
