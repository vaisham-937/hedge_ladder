# Algo App Vash

FastAPI + Zerodha (Kite) based trading dashboard with:
- Live tick streaming
- Chartink webhook ingestion
- Ladder engine (auto/manual)
- Auto square-off at **15:20 IST**
- Daily reset at **08:00 IST**

## 1. Tech Stack

- Python (FastAPI)
- Redis (state/cache/session-like app data)
- Zerodha Kite Connect + KiteTicker
- Frontend: `templates/dashboard.html` (vanilla JS + Tailwind-style classes)

## 2. Project Files

- `main.py`: FastAPI app, APIs, websocket routes, background schedulers
- `ladder_engine.py`: ladder trading core logic
- `kite_ws_worker.py`: separate Kite websocket worker process
- `fetch_nse_cash_instruments.py`: build local symbol-token map file
- `templates/dashboard.html`: dashboard UI
- `nse_eq_instruments.json`: local instrument map used by backend

## 3. Prerequisites

- Python 3.10+
- Redis running on `127.0.0.1:6379` (DB 0)
- Zerodha API credentials

Install deps:

```powershell
pip install -r requirements.txt
```

## 4. How to Start (Exact Flow)

### Step A: Start Redis

Make sure Redis is running at:
- Host: `127.0.0.1`
- Port: `6379`
- DB: `0`

### Step B: Start FastAPI app (dashboard backend)

```powershell
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

App URL:
- `http://127.0.0.1:8000/dashboard`

### Step C: Login/connect Zerodha from dashboard

1. Open dashboard  
2. Save API key + secret  
3. Click connect (OAuth callback stores access token in Redis)

Redis keys created:
- `api_key:<user_id>`
- `access_token:<user_id>`
- `zerodha:keys:<user_id>` (hash)

### Step D: Start Kite WS worker (separate terminal)

```powershell
python kite_ws_worker.py --user-id 1
```

This worker pushes ticks/order-updates to backend ingest websocket:
- `ws://127.0.0.1:8000/ws/ingest?user_id=1`

### Step E (Recommended): Build instrument file

```powershell
python fetch_nse_cash_instruments.py --user-id 1
```

Creates:
- `nse_eq_instruments.json`

Used for symbol ↔ token mapping before first live tick arrives.

## 5. Port and URLs

- Backend HTTP: `127.0.0.1:8000`
- Dashboard page: `http://127.0.0.1:8000/dashboard`
- Dashboard ticks WS: `ws://127.0.0.1:8000/ws/ticks?user_id=<id>`
- Ingest WS (worker -> backend): `ws://127.0.0.1:8000/ws/ingest?user_id=<id>`

## 6. Chartink Webhook URL

Use this format:

```text
https://<your-ngrok-domain>/webhook/chartink?user_id=1
```

Example:

```text
https://xxxx.ngrok-free.dev/webhook/chartink?user_id=1
```

Notes:
- Supports JSON and form payloads
- `user_id` can come from query param or payload
- If omitted, defaults to `1`

## 7. API Endpoints (Complete)

### WS token control
- `GET /api/ws-tokens?user_id=<id>`  
  Worker token list (no session required)
- `GET /api/ws-tokens/session`  
  Browser/session version

### Dashboard/UI
- `GET /dashboard`  
  Serves dashboard HTML, sets session `user_id=1` if missing

### Zerodha credentials/auth
- `POST /api/save-credentials`  
  Body: `{ "api_key": "...", "api_secret": "..." }`
- `GET /connect/zerodha`  
  Redirects to Zerodha login
- `GET /zerodha/callback`  
  Receives `request_token`, stores access token in Redis
- `GET /api/zerodha-status`  
  Connected + token expiry info

### Automation mode
- `POST /api/automation-mode`  
  Body: `{ "mode": "MANUAL" | "AUTO" }`
- `GET /api/automation-mode`

### Universal settings
- `POST /api/universal-settings`  
  Body: `{ "settings": { ... } }` (required fields validated)
- `GET /api/universal-settings`

### Stock-wise settings
- `POST /api/stock-settings`  
  Body: `{ "symbol": "SBIN", "settings": { ... } }`
- `GET /api/stock-settings?symbol=SBIN`
- `GET /api/stock-settings/list`
- `DELETE /api/stock-settings`  
  Body: `{ "symbol": "SBIN" }`

### Market/circuit
- `GET /api/market-status`
- `GET /api/circuit/{symbol}`

### Chartink alerts
- `POST /webhook/chartink`
- `GET /api/alerts`

### Ladder engine
- `POST /api/ladder/start`  
  Body (model `LadderStartReq`):
  - `symbol` (str)
  - `side` (`BUY`/`SELL`)
  - `settings_mode` (`UNIVERSAL`/`STOCK`, optional)
  - `settings` (optional override)
- `POST /api/ladder/squareoff`  
  Body: `{ "symbol": "SBIN" }`
- `GET /api/ladder/sessions`
- `GET /api/ladder/state`
- `POST /api/ladder/delete?symbol=SBIN`

### Risk/exit controls
- `POST /api/kill-switch`  
  Body: `{ "enabled": true|false }`
- `POST /api/squareoff` (single symbol)  
  Body: `{ "symbol": "SBIN" }`
- `POST /api/squareoff_all`
- `GET /api/auto-squareoff-event`  
  Dashboard polling endpoint for auto square-off highlight event

### Manual trade route
- `POST /api/trade`  
  Body: `{ "symbol": "...", "side": "BUY|SELL", "qty": 1 }`  
  Includes LTP band validation: **100–4000**

## 8. Websocket Message Flow

## 8.1 Worker -> backend ingest (`/ws/ingest`)

Tick payload:

```json
{
  "type": "ticks",
  "ticks": [
    {
      "token": 738561,
      "symbol": "RELIANCE",
      "ltp": 2910.5,
      "high": 2940.0,
      "low": 2890.0,
      "prev": 2900.0,
      "tbq": 10000,
      "tsq": 9000,
      "volume": 1200000,
      "ts": 1739090000
    }
  ]
}
```

Order update payload:

```json
{
  "type": "order_update",
  "payload": { "order_id": "...", "status": "COMPLETE" }
}
```

## 8.2 Backend -> dashboard (`/ws/ticks`)

- Sends immediate `snapshot` on connect
- Then symbol-wise incremental tick broadcasts

## 9. Scheduled Background Jobs

Started in `@app.on_event("startup")`:
- `circuit_prefetch_loop()`
- `auto_squareoff_loop()`
- `daily_reset_loop()`

### Auto square-off
- Time: **15:20 IST** (weekdays)
- Action: square-off all users having `access_token:*`
- Stores event key for UI: `auto_squareoff:event:<user_id>`

### Daily reset
- Time: **08:00 IST**
- Clears keys like:
  - auth tokens
  - automation mode
  - universal/stock settings
  - ladder state/locks/count
  - chartink alerts
  - ws tokens

## 10. Important Redis Keys

Auth/session:
- `zerodha:keys:<uid>` (hash: api_key, api_secret)
- `api_key:<uid>`
- `access_token:<uid>`

Automation/settings:
- `automation:mode:<uid>`
- `universal:settings:<uid>`
- `stock:settings:<uid>:<symbol>`

Ladder:
- `ladder:state:<uid>:<symbol>`
- `ladder:lock:<uid>:<symbol>:entry|add|exit`
- `ladder:count:<uid>:<symbol>`
- `ladder:global_count:<uid>`

Alerts/webhook:
- `chartink_alerts:<uid>:<YYYY-MM-DD>`
- `chartink_seen:<uid>:<YYYY-MM-DD>:<scan_name>`

WS + symbol map/cache:
- `ws:<uid>:tokens` (some flows)
- `circuit:<symbol>`
- `open:<YYYYMMDD>:<symbol>`
- `symbol_token:<symbol>` (optional usage in cleanup paths)

## 11. Troubleshooting

### 1) Alerts webhook 200 but dashboard empty
- Check `GET /api/alerts` response
- Confirm webhook has symbols field or parseable payload
- Ensure `user_id` mapping correct in webhook URL

### 2) `START_BLOCKED_GLOBAL_LIMIT`
- Means `ladder:global_count:<uid>` reached `max_trades_per_symbol` limit logic context.
- Clear relevant count keys only if you intentionally want reset.

### 3) `START_LADDER already_running`
- Symbol session still active in memory or Redis state.
- Check `/api/ladder/state`, square-off/delete stale session.

### 4) Worker reconnect loop (`/ws/ingest`)
- Verify FastAPI is up on port 8000
- Verify worker user-id and token keys in Redis

### 5) Dashboard stale PNL after square-off
- Frontend now clears stale ladder meta when table empties.
- Hard refresh (`Ctrl+F5`) after deploy.

## 12. Quick Start Commands (Copy-Paste)

```powershell
# 1) install deps
pip install -r requirements.txt

# 2) run backend
uvicorn main:app --host 127.0.0.1 --port 8000 --reload

# 3) (optional) refresh instruments
python fetch_nse_cash_instruments.py --user-id 1

# 4) run ws worker
python kite_ws_worker.py --user-id 1
```

Open:
- `http://127.0.0.1:8000/dashboard`

## 13. End-to-End Data Workflow (Code-Level)

This is the full runtime flow from external data to dashboard values:

1. Chartink hits webhook:
   - `POST /webhook/chartink?user_id=<id>`
   - Backend parses payload, normalizes symbols, stores alert packet in Redis list `chartink_alerts:<uid>:<date>`.
2. Dashboard polls alerts:
   - `GET /api/alerts` every ~15s
   - Rebuilds rows only when payload signature changes (`LAST_ALERTS_RENDER`) to reduce UI blinking.
3. Dashboard symbol subscription list:
   - Every symbol present in alerts table is tracked in `subscribed` set.
4. Tick stream ingestion:
   - `kite_ws_worker.py` connects KiteTicker and receives full ticks.
   - Worker sends batches to backend via websocket:
     - `ws://127.0.0.1:8000/ws/ingest?user_id=<id>`
5. Backend tick normalization:
   - In `main.py` `ws_ingest`: token/symbol, OHLC, TBQ/TSQ, volume normalized.
   - Symbol-token RAM maps updated:
     - `SYMBOL_TOKEN_RAM[user][symbol] = token`
     - `TOKEN_SYMBOL_RAM[user][token] = symbol`
   - Tick stored in `TICK_HUB` (in-memory).
6. Backend dashboard broadcast:
   - Backend pushes symbol tick updates to all dashboard ws clients on:
     - `/ws/ticks`
7. Frontend tick flush:
   - Incoming ticks buffered in `TICK_BUFFER`
   - `flushTickUI()` updates all `data-field` cells without full table rebuild.
8. Ladder updates:
   - Same ingest ticks are passed to engine `eng.ingest_tick(token, ltp)` if engine exists.
   - Active ladder rows are polled from `/api/ladder/state`.
   - LTP + PnL update continuously via websocket ticks and `LADDER_META`.

## 14. Websocket Lifecycle (Detailed)

### 14.1 Worker Side (`kite_ws_worker.py`)

- Reads auth from Redis:
  - `api_key:<uid>`
  - `access_token:<uid>`
- Connects KiteTicker.
- `sync_tokens()` polls FastAPI:
  - `GET /api/ws-tokens?user_id=<uid>`
- Subscribes new tokens in Kite:
  - `kws.subscribe([...])`
  - `kws.set_mode(kws.MODE_FULL, [...])`
- Sends to backend ingest WS in batches:
  - `{"type":"ticks","ticks":[...]}`
  - `{"type":"order_update","payload":{...}}`

### 14.2 Backend Ingest (`main.py` `/ws/ingest`)

- Accepts worker websocket.
- For each tick:
  - validates token/ltp
  - resolves symbol (payload -> RAM map -> global map)
  - enriches open/high/low/prev
  - falls back open price via `get_cached_open_price()`
  - updates `TICK_HUB`
  - calls ladder engine ingest
  - broadcasts to dashboard clients

### 14.3 Dashboard WS (`main.py` `/ws/ticks`)

- On connect: sends `snapshot` (latest known ticks) immediately.
- During session: receives symbol tick updates and updates table cells.

## 15. Value Calculation Reference (Exact Formulas)

These formulas are implemented in `templates/dashboard.html` (`flushTickUI`, `calcUnrealizedPnl`, `updateNetPNL`).

### 15.1 Live Signals Table

- `LTP`:
  - `ltp.toFixed(2)`
- `OPEN`:
  - uses tick open if available; else `--`
- `PRVCLOSE`:
  - `prev` from tick (`prev`/`prev_close`/`ohlc.close`)
- `% CHG`:
  - `((ltp - prev) / prev) * 100`
- `FR O` (From Open %):
  - `((ltp - open) / open) * 100`
- `FR H` (From High %):
  - `((ltp - high) / high) * 100`
- `FR L` (From Low %):
  - `((ltp - low) / low) * 100`
- `TBQ`:
  - `d.tbq` formatted with Indian commas
- `TSQ`:
  - `d.tsq` formatted with Indian commas
- `VOLUME`:
  - `d.volume` formatted with Indian commas
- `TURN (CR)`:
  - `tcr = (ltp * volume) / 10000000`
- `BUY V`:
  - `Math.round(ltp * tbq)` formatted
- `SELL V`:
  - `Math.round(ltp * tsq)` formatted
- `UC`/`LC`:
  - from cached circuit values (`upper_circuit`, `lower_circuit`)

### 15.2 Active Ladders Table

- Row `LTP`:
  - live from ws ticks (`data-lsym`, `data-lfield="ltp"`)
- `PnL` per symbol (`calcUnrealizedPnl`):
  - BUY leg: `(ltp - avg) * qty`
  - SELL leg: `(avg - ltp) * qty`
- `NET PNL` (`updateNetPNL`):
  - sum of all active symbol unrealized PnL in `LADDER_META`
- `NEXT ADD`:
  - BUY: `last_add_price * (1 + threshold_pct/100)`
  - SELL: `last_add_price * (1 - threshold_pct/100)`
- `SL`:
  - BUY: `avg * (1 - stop_loss_pct/100)`
  - SELL: `avg * (1 + stop_loss_pct/100)`
- `TSL`:
  - BUY: `highest * (1 - trailing_sl_pct/100)`
  - SELL: `lowest * (1 + trailing_sl_pct/100)`
- `QTY progress`:
  - `${min(entries,max_adds)} / ${max_adds}`

## 16. Alert -> Auto Trade Workflow

When mode is `AUTO`:

1. Alerts are polled.
2. New symbols are detected (`newSymbols` vs previous set).
3. Symbols existing before AUTO was enabled are ignored (`automationEnabledSymbols` baseline).
4. Universal settings fetched (`GET /api/universal-settings`).
5. For each valid new symbol:
   - `POST /api/ladder/start` with `settings_mode="UNIVERSAL"`
6. On success symbol is tracked in `processedAutoTradeSymbols`.
7. Ladder table refresh runs after auto starts.

## 17. Ladder State Sources

`GET /api/ladder/state` priority:

1. In-memory live engine sessions (`ENGINE_HUB[user].list_sessions()`)
2. Redis fallback keys `ladder:state:<uid>:*`

This is why sometimes data appears even after restart if Redis state still exists.

## 18. Open Price Source Priority

Backend function `get_cached_open_price(user_id, symbol)`:

1. RAM cache `OPEN_RAM`
2. Redis day key `open:<YYYYMMDD>:<SYMBOL>`
3. Kite quote fallback (`ohlc.open`)

This keeps `OPEN` and `FR O` stable even when incoming tick lacks open.

## 19. Auto Square-off UI Event Workflow

1. Scheduler `auto_squareoff_loop()` triggers at 15:20 IST.
2. Calls `execute_squareoff_all(user_id)`.
3. On success writes event key:
   - `auto_squareoff:event:<uid>`
4. Dashboard polls:
   - `GET /api/auto-squareoff-event`
5. Shows highlighted toast:
   - `AUTO SQUARE-OFF TRIGGERED (...)`

## 20. Practical Debug Checklist (Data Not Moving)

If values not updating:

1. Check worker running:
   - `python kite_ws_worker.py --user-id 1`
2. Check ingest WS logs in backend (`/ws/ingest connected`).
3. Verify dashboard WS connected (`/ws/ticks`).
4. Check `/api/ws-tokens?user_id=1` returns tokens.
5. Check `/api/alerts` has rows.
6. Check browser console for JS errors.
7. Check Redis keys exist (auth/settings/alerts/state).
