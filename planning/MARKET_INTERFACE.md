# Market Data Interface — Unified Price API

The design for the project's single price-retrieval abstraction: one interface, two
interchangeable implementations (**Massive API** when `MASSIVE_API_KEY` is set,
**GBM simulator** otherwise), with all downstream code reading prices from a shared
in-memory cache rather than from either source directly.

See `MASSIVE_API.md` for the real-data backend and `MARKET_SIMULATOR.md` for the
simulator.

---

## 1. Design goals

| Goal | How it's met |
|------|--------------|
| Source-agnostic downstream code | Everything reads the `PriceCache`; nothing calls a data source for a price. |
| Drop-in swappable backends | Both backends implement one ABC; a factory picks one from an env var. |
| Non-blocking | Each backend owns a background `asyncio` task; the sync Massive SDK runs in a thread. |
| Multi-user ready | The cache is keyed by ticker, not user; SSE fans out to all clients. |
| Testable offline | No API key, no network → simulator. `LLM_MOCK`-style determinism for tests. |

---

## 2. Architecture

```
                 env: MASSIVE_API_KEY?
                          │
              ┌───────────┴───────────┐
              │  create_market_data_source(cache)  │   ← factory
              └───────────┬───────────┘
                  set ?   │   not set
            ┌─────────────┘             └─────────────┐
            ▼                                         ▼
   MassiveDataSource                          SimulatorDataSource
   (polls Massive REST                        (GBM background loop
    every ~15s, in a thread)                   every ~500ms)
            │                                         │
            └──────────────► PriceCache ◄─────────────┘
                            (thread-safe,
                             latest price/ticker)
                                  │
                                  ▼
                        SSE  GET /api/stream/prices
                                  │
                                  ▼
                       portfolio valuation, trade fills,
                       frontend EventSource clients
```

**Key principle:** data sources are *producers* that write to the cache on their own
schedule. Consumers (SSE, portfolio math, trade execution) are *readers* of the
cache. The two never call each other directly. This decoupling is what makes the
backend swap invisible to the rest of the app.

---

## 3. The `MarketDataSource` interface

An abstract base class defining the lifecycle every backend must support.

```python
from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates. Starts the background task. Call once."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources. Safe to call repeatedly."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set; also evict it from the cache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the currently tracked tickers."""
```

Design notes:

- **`start`/`stop` are async** because both backends own asyncio tasks and the
  Massive backend does an immediate first fetch on `start` (so the cache is warm
  before the first SSE event).
- **`add_ticker`/`remove_ticker` are async** for interface symmetry even though the
  simulator's work is synchronous — it keeps callers uniform and leaves room for a
  future backend that must make a network call to subscribe.
- **`get_tickers` is sync** — it's a cheap in-memory read.
- `remove_ticker` is responsible for cache eviction so a removed watchlist entry
  stops streaming immediately.

---

## 4. `PriceUpdate` — the wire model

An immutable snapshot of one ticker at one instant. It is the single shape that
flows from cache → SSE → frontend, regardless of backend.

```python
from dataclasses import dataclass, field
import time


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:            # absolute move from previous update
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:    # % move; 0 when previous is 0
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:           # "up" | "down" | "flat" — drives flash color
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:            # JSON for SSE
        ...
```

`direction`/`change_percent` exist so the frontend doesn't recompute them and the
green/red flash logic is centralized. `timestamp` is always **Unix seconds** — the
Massive backend converts the SDK's ms/ns timestamps to seconds before caching.

---

## 5. `PriceCache` — the shared store

The decoupling point. Thread-safe because the Massive backend writes from a worker
thread (`asyncio.to_thread`) while SSE/portfolio read from the event loop.

```python
class PriceCache:
    """Thread-safe in-memory cache of the latest price per ticker.

    Writers: exactly one data source at a time.
    Readers: SSE endpoint, portfolio valuation, trade execution.
    """

    def update(self, ticker, price, timestamp=None) -> PriceUpdate:
        """Record a new price. Computes change/direction vs. the prior value.
        First update for a ticker → previous_price == price (direction 'flat').
        Bumps a monotonic version counter."""

    def get(self, ticker) -> PriceUpdate | None: ...
    def get_price(self, ticker) -> float | None: ...
    def get_all(self) -> dict[str, PriceUpdate]: ...   # shallow copy
    def remove(self, ticker) -> None: ...

    @property
    def version(self) -> int: ...   # increments on every update → SSE change detection
```

Why a `version` counter: the SSE generator only serializes and pushes when
`version` changed since the last tick, avoiding redundant identical frames.

---

## 6. The factory — env-driven backend selection

The one place that knows about both concrete classes. Selection rule mirrors the
PLAN exactly:

```python
import os


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set & non-empty → MassiveDataSource; else SimulatorDataSource.
    Returns an UNSTARTED source; caller must `await source.start(tickers)`."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    logger.info("Market data source: GBM Simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

`.strip()` matters: a present-but-blank `MASSIVE_API_KEY=` in `.env` correctly falls
through to the simulator instead of constructing a client with an empty key.

---

## 7. The Massive backend (shape)

Implements the interface by polling the snapshot endpoint and writing to the cache.
Full API details in `MASSIVE_API.md`.

```python
class MassiveDataSource(MarketDataSource):
    def __init__(self, api_key, price_cache, poll_interval=15.0):
        ...  # 15s default is free-tier safe (one snapshot call covers all tickers)

    async def start(self, tickers):
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()                       # warm the cache immediately
        self._task = asyncio.create_task(self._poll_loop())

    async def _poll_once(self):
        if not self._tickers or not self._client:
            return
        try:
            # SDK is synchronous → run off the event loop
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            for snap in snapshots:
                try:
                    self._cache.update(
                        ticker=snap.ticker,
                        price=snap.last_trade.price,
                        timestamp=snap.last_trade.timestamp / 1000.0,  # ms → s
                    )
                except (AttributeError, TypeError):
                    continue   # skip one malformed snapshot, keep the batch
        except Exception:
            logger.error(...)   # never crash the loop (401/429/network)
```

Two-level error handling — batch-level (network/auth/rate-limit) and per-ticker
(malformed record) — keeps the poller alive indefinitely. `add_ticker` just appends
to the list; the ticker appears on the next poll. `remove_ticker` drops it and
evicts the cache entry.

---

## 8. The simulator backend (shape)

Implements the same interface with an in-process GBM loop on a ~500 ms cadence.
Detailed in `MARKET_SIMULATOR.md`.

```python
class SimulatorDataSource(MarketDataSource):
    async def start(self, tickers):
        self._sim = GBMSimulator(tickers=tickers)
        for t in tickers:                       # seed cache so SSE has data at t=0
            self._cache.update(t, self._sim.get_price(t))
        self._task = asyncio.create_task(self._run_loop())

    async def _run_loop(self):
        while True:
            prices = self._sim.step()           # advance all tickers one step
            for ticker, price in prices.items():
                self._cache.update(ticker, price)
            await asyncio.sleep(self._interval)  # ~0.5s
```

---

## 9. Wiring into FastAPI

```python
from app.market import PriceCache, create_market_data_source, create_stream_router

price_cache = PriceCache()
source = create_market_data_source(price_cache)

@app.on_event("startup")
async def _startup():
    await source.start(load_watchlist_tickers())

@app.on_event("shutdown")
async def _shutdown():
    await source.stop()

app.include_router(create_stream_router(price_cache))  # GET /api/stream/prices
```

When the watchlist changes (manual edit or via the AI chat), the same `source` is
mutated: `await source.add_ticker("PYPL")` / `await source.remove_ticker("V")`.
Trade execution and portfolio valuation call `price_cache.get_price(ticker)` — they
neither know nor care which backend is running.

---

## 10. Public surface (import contract)

```python
from app.market import (
    PriceUpdate,
    PriceCache,
    MarketDataSource,
    create_market_data_source,
    create_stream_router,
)
```

Internal classes (`MassiveDataSource`, `SimulatorDataSource`, `GBMSimulator`) are
implementation detail — consumers should depend only on the names above plus the
factory.
