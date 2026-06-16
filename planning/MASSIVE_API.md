# Massive API — Reference & Integration Guide

Research notes and code examples for retrieving real-time and end-of-day stock
prices for multiple tickers from **Massive** (formerly **Polygon.io**).

> **Rebrand note:** Polygon.io rebranded to **Massive** on Oct 30, 2025. Existing
> API keys, accounts, and integrations continue to work unchanged. The REST host
> moved from `api.polygon.io` to `api.massive.com` (the old host still resolves for
> an extended deprecation period), and the Python SDK package renamed from
> `polygon-api-client` to `massive`. Everything below uses the new naming.

---

## 1. Account, Plans & Authentication

### API key

A single API key authenticates every request. In this project it is read from the
`MASSIVE_API_KEY` environment variable (see `.env`). If the variable is unset or
empty, the app falls back to the built-in simulator and never touches this API.

Authentication can be supplied two ways — pick one:

```bash
# Bearer header (preferred)
curl -H "Authorization: Bearer $MASSIVE_API_KEY" \
  "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,TSLA"

# Query parameter (handy for quick tests)
curl "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,TSLA&apiKey=$MASSIVE_API_KEY"
```

The Python SDK handles the header for you when you pass `api_key=...`.

### Plans, rate limits & latency

| Plan        | Rate limit       | Quote/trade latency        | Suggested poll interval |
|-------------|------------------|----------------------------|-------------------------|
| Free/Basic  | 5 requests/min   | 15-min delayed (EOD focus) | 15 s                    |
| Starter     | higher           | 15-min delayed             | 5–15 s                  |
| Developer   | higher           | 15-min delayed             | 2–5 s                   |
| Advanced/Business | very high  | real-time                  | 2–5 s                   |

> The free tier's 5 req/min is the binding constraint for most users of this
> project. Because we fetch **all watched tickers in one snapshot call**, a 15 s
> poll interval uses only 4 req/min — comfortably under the limit. This is the
> default in the integration (`poll_interval=15.0`).

---

## 2. Why the snapshot endpoint (not per-ticker calls)

This project tracks ~10 tickers and needs to refresh them all on a fixed cadence.
The naive approach — one request per ticker — would blow the free-tier rate limit
almost immediately (10 tickers × 4 polls/min = 40 req/min > 5).

The **Full Market Snapshot** endpoint solves this: it returns the latest data for a
comma-separated list of tickers (or the entire market) in a **single request**, so
the request count is independent of watchlist size.

### Endpoint

```
GET /v2/snapshot/locale/us/markets/stocks/tickers
```

Full URL: `https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers`

| Query param   | Type    | Required | Notes |
|---------------|---------|----------|-------|
| `tickers`     | string  | No       | Case-sensitive comma-separated list, e.g. `AAPL,TSLA,GOOGL`. Empty → all ~10,000 tickers. |
| `include_otc` | boolean | No       | Include OTC securities. Default `false`. |

> **Daily reset:** snapshot data is cleared at **3:30 AM EST** and repopulates as
> exchanges report, starting around **4:00 AM EST**. Before repopulation, `day`
> values may be zero/stale; `prevDay` is the reliable fallback (see §5).

### Example request

```bash
curl -H "Authorization: Bearer $MASSIVE_API_KEY" \
  "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,TSLA"
```

### Example response

```json
{
  "status": "OK",
  "count": 1,
  "tickers": [
    {
      "ticker": "AAPL",
      "todaysChange": -0.124,
      "todaysChangePerc": -0.601,
      "updated": 1605192894630916600,
      "day":      { "o": 20.64, "h": 20.64, "l": 20.50, "c": 20.50, "v": 37216, "vw": 20.61 },
      "lastTrade":{ "p": 20.50, "s": 2416, "t": 1605192894630916600, "c": [14, 41], "x": 4, "i": "71675577320245" },
      "lastQuote":{ "P": 20.6, "S": 22, "p": 20.5, "s": 13, "t": 1605192959994246100 },
      "min":      { "o": 20.50, "h": 20.50, "l": 20.50, "c": 20.50, "v": 5000, "vw": 20.50 },
      "prevDay":  { "o": 20.79, "h": 21.00, "l": 20.50, "c": 20.63, "v": 292738, "vw": 20.69 }
    }
  ]
}
```

### Field reference (the ones we use)

| Path                 | Meaning |
|----------------------|---------|
| `ticker`             | Symbol. |
| `lastTrade.p`        | **Last trade price** — the live price we cache. |
| `lastTrade.t`        | Trade timestamp, **Unix nanoseconds** (or ms in older payloads — convert carefully). |
| `lastTrade.s`        | Trade size (shares). |
| `day.c` / `day.o`    | Today's close-so-far / open. |
| `prevDay.c`          | Previous session's close — used for "daily change %" and as a fallback price. |
| `todaysChange[Perc]` | Server-computed change vs. prev close (convenient, but we compute our own). |
| `min`                | Most recent minute aggregate bar. |

> **Timestamp gotcha:** Massive returns nanoseconds in most v2/v3 trade/quote
> fields. The SDK normalizes some of these. In our client we treat the SDK's
> `last_trade.timestamp` as **milliseconds** and divide by 1000 to get seconds —
> verify against your plan's payload and adjust the divisor if values look wrong
> (a price timestamp 1000× in the future is the tell-tale sign).

---

## 3. Python SDK

The official client is the `massive` package.

```bash
pip install -U massive      # requires Python 3.9+
# or, in this uv project:
uv add massive
```

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key="<MASSIVE_API_KEY>")  # defaults to api.massive.com
```

### Snapshot for multiple tickers (the hot path)

```python
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "TSLA"],
)

for snap in snapshots:
    print(snap.ticker, snap.last_trade.price, snap.last_trade.timestamp)
```

Each returned object exposes snake_case attributes (the SDK maps the JSON for you):

| JSON field    | SDK attribute            |
|---------------|--------------------------|
| `lastTrade.p` | `snap.last_trade.price`  |
| `lastTrade.t` | `snap.last_trade.timestamp` |
| `lastTrade.s` | `snap.last_trade.size`   |
| `day.c`       | `snap.day.close`         |
| `prevDay.c`   | `snap.prev_day.close`    |
| `todaysChange`| `snap.todays_change`     |

> The SDK's `RESTClient` is **synchronous**. In our async FastAPI app we call it via
> `asyncio.to_thread(...)` so it never blocks the event loop (see
> `MARKET_INTERFACE.md`).

---

## 4. Real-time vs. end-of-day

This project polls the **snapshot** endpoint on an interval for "real-time-ish"
prices — appropriate because:

- SSE pushes to the browser at ~500 ms, but the *underlying* data only changes as
  fast as our poll interval (≥2 s). The frontend interpolates/animates between.
- A WebSocket feed (`wss://socket.massive.com`) exists for true tick streaming, but
  it requires a real-time plan and adds connection-management complexity that the
  one-shot snapshot poll avoids. **We deliberately use REST polling**, matching the
  PLAN's "REST API polling (not WebSocket) — simpler, works on all tiers."

For **end-of-day** / historical needs (e.g., seeding charts or backfilling), use the
aggregate endpoints rather than snapshots:

### Previous-day bar (one ticker)

```
GET /v2/aggs/ticker/{ticker}/prev
```

```python
prev = client.get_previous_close_agg(ticker="AAPL")
print(prev[0].close, prev[0].open, prev[0].volume)
```

### Daily market summary / grouped daily (every ticker, one date)

```
GET /v2/aggs/grouped/locale/us/market/stocks/{date}
```

```python
bars = client.get_grouped_daily_aggs(date="2026-06-15")
for bar in bars:
    print(bar.ticker, bar.close)
```

One request returns OHLCV + VWAP for **every** US ticker on that date — ideal for a
nightly EOD snapshot without N requests.

### Custom bars / aggregates (historical series for one ticker)

```
GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
```

```python
bars = client.get_aggs(ticker="AAPL", multiplier=1, timespan="day",
                       from_="2026-01-01", to="2026-06-15")
```

---

## 5. Error handling & resilience (what the integration must do)

The poller runs forever in the background; a transient API hiccup must **never**
crash it. Handle these cases:

| Condition            | HTTP | Strategy |
|----------------------|------|----------|
| Bad/expired key      | 401  | Log once, keep looping (or surface a health flag). Don't crash. |
| Rate limit exceeded  | 429  | Back off; ensure poll interval respects the plan's req/min. |
| Network error/timeout| —    | Catch, log, retry on next interval. |
| Malformed/partial snapshot | 200 | Skip the individual ticker (missing `last_trade`), keep the rest. |
| Pre-open / data reset| 200  | `day`/`lastTrade` may be empty; fall back to `prevDay.close`. |

The implementation wraps each poll in a broad `try/except` that logs and returns,
so the loop always survives to the next tick. Per-ticker parsing is wrapped
separately so one bad record doesn't drop the whole batch.

---

## 6. How this project uses it (summary)

- **One** snapshot call per poll covers the entire watchlist → rate-limit-friendly.
- Default **15 s** interval (free tier safe); configurable down to 2–5 s on paid plans.
- Synchronous SDK call offloaded to a thread; results written to the shared
  `PriceCache`; SSE reads the cache. The rest of the app is agnostic to whether the
  data came from Massive or the simulator.

See `MARKET_INTERFACE.md` for the unified abstraction and `MARKET_SIMULATOR.md` for
the offline fallback.

---

### Sources

- [Polygon.io is Now Massive](https://massive.com/blog/polygon-is-now-massive)
- [Full Market Snapshot — Stocks REST API](https://massive.com/docs/stocks/get_v2_snapshot_locale_us_markets_stocks_tickers)
- [Stocks REST API Overview](https://massive.com/docs/rest/stocks/overview)
- [Previous Day Bar (OHLC)](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar)
- [Daily Market Summary (Grouped Daily)](https://massive.com/docs/rest/stocks/aggregates/daily-market-summary)
- [massive-com/client-python (official Python SDK)](https://github.com/massive-com/client-python)
- [client-python Getting Started](https://deepwiki.com/massive-com/client-python/2-getting-started)
