# Market Data Backend — Detailed Design

Implementation-ready design for the FinAlly market data subsystem. It covers the
unified data-source interface, the in-memory price cache, the GBM simulator, the
Massive (Polygon.io) REST client, the SSE streaming endpoint, and how all of it
wires into FastAPI's application lifecycle.

Everything described here lives under `backend/app/market/`. The snippets are the
real implementation — this document is both the design rationale and a faithful
reference to the shipped code.

> Companion docs: `MARKET_INTERFACE.md` (the abstraction), `MARKET_SIMULATOR.md`
> (the GBM math), `MASSIVE_API.md` (the real-data API reference), and
> `MARKET_DATA_SUMMARY.md` (status & test coverage).

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [File Structure & Module Map](#2-file-structure--module-map)
3. [Data Flow Overview](#3-data-flow-overview)
4. [Data Model — `models.py`](#4-data-model--modelspy)
5. [Price Cache — `cache.py`](#5-price-cache--cachepy)
6. [Abstract Interface — `interface.py`](#6-abstract-interface--interfacepy)
7. [Seed Prices & Parameters — `seed_prices.py`](#7-seed-prices--parameters--seed_pricespy)
8. [GBM Simulator — `simulator.py`](#8-gbm-simulator--simulatorpy)
9. [Massive API Client — `massive_client.py`](#9-massive-api-client--massive_clientpy)
10. [Factory — `factory.py`](#10-factory--factorypy)
11. [SSE Streaming Endpoint — `stream.py`](#11-sse-streaming-endpoint--streampy)
12. [Public Surface — `__init__.py`](#12-public-surface--__init__py)
13. [FastAPI Lifecycle Integration](#13-fastapi-lifecycle-integration)
14. [Watchlist Coordination](#14-watchlist-coordination)
15. [Error Handling & Edge Cases](#15-error-handling--edge-cases)
16. [Testing Strategy](#16-testing-strategy)
17. [Configuration Summary](#17-configuration-summary)

---

## 1. Design Principles

| Principle | How it's realized |
|-----------|-------------------|
| **Source-agnostic downstream** | Nothing outside `market/` calls a data source for a price. SSE, portfolio valuation and trade fills all read `PriceCache`. |
| **Drop-in swappable backends** | `SimulatorDataSource` and `MassiveDataSource` implement one ABC. A factory picks one from `MASSIVE_API_KEY`. |
| **Producer/consumer decoupling** | Data sources are *producers* writing to the cache on their own schedule; everything else is a *reader*. The two never call each other. |
| **Non-blocking** | Each backend owns a background `asyncio` task. The synchronous Massive SDK is offloaded with `asyncio.to_thread`. |
| **Resilient loops** | Both background loops wrap their work in `try/except` so a transient failure (network, malformed record, math error) never kills the stream. |
| **Multi-user ready** | The cache is keyed by ticker, not user; SSE fans the same cache out to all connected clients. |
| **Testable offline** | No key, no network → simulator. Determinism via seeding `random`/`np.random`. |

---

## 2. File Structure & Module Map

```
backend/app/market/
├── __init__.py          # Public re-exports (the import contract)
├── models.py            # PriceUpdate — the immutable wire model
├── cache.py             # PriceCache — thread-safe in-memory store
├── interface.py         # MarketDataSource — abstract base class
├── seed_prices.py       # SEED_PRICES, TICKER_PARAMS, correlation constants
├── simulator.py         # GBMSimulator (math) + SimulatorDataSource (lifecycle)
├── massive_client.py    # MassiveDataSource — Polygon.io REST poller
├── factory.py           # create_market_data_source() — env-driven selection
└── stream.py            # create_stream_router() — FastAPI SSE endpoint
```

| Module | Owns | Depends on |
|--------|------|-----------|
| `models.py` | `PriceUpdate` | stdlib only |
| `cache.py` | `PriceCache` | `models` |
| `interface.py` | `MarketDataSource` ABC | stdlib only |
| `seed_prices.py` | constants | none |
| `simulator.py` | `GBMSimulator`, `SimulatorDataSource` | `cache`, `interface`, `seed_prices`, `numpy` |
| `massive_client.py` | `MassiveDataSource` | `cache`, `interface`, `massive` |
| `factory.py` | `create_market_data_source` | `cache`, `interface`, both sources, `os` |
| `stream.py` | `create_stream_router` | `cache`, `fastapi` |

Layering is strictly downward: `models → cache → {sources, stream} → factory`.
No cycles.

---

## 3. Data Flow Overview

```
                    env: MASSIVE_API_KEY?
                            │
              create_market_data_source(cache)        ← factory
                            │
              set & non-empty │ unset / blank
            ┌───────────────┘         └───────────────┐
            ▼                                          ▼
    MassiveDataSource                          SimulatorDataSource
  (poll snapshot every ~15s,                  (GBM step every ~500ms,
   SDK call in a thread)                       in-process numpy)
            │                                          │
            └───────────────►  PriceCache  ◄───────────┘
                              (thread-safe,
                          latest PriceUpdate/ticker,
                            monotonic version#)
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                     ▼
   SSE GET /api/stream/prices   portfolio          trade execution
   (version-gated push)         valuation          (fill price lookup)
              │
              ▼
   browser EventSource clients (price flash, sparklines)
```

The **single arrow into the cache** from exactly one active source, and the
**fan-out of readers** from it, is the architectural keystone. Swapping the
backend changes only what writes to the cache; every reader is untouched.

---

## 4. Data Model — `models.py`

`PriceUpdate` is the single shape that flows cache → SSE → frontend, regardless of
which backend produced it. It is a **frozen, slotted dataclass** — immutable (safe
to share across threads and SSE clients) and memory-cheap.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design notes

- **`change`, `change_percent`, `direction` are derived properties**, not stored
  fields. The frontend never recomputes them; the green/red flash decision is
  centralized here (`direction`).
- **`change_percent` is tick-over-tick** (vs. the previous cached price), *not*
  vs. the previous trading day. Daily change % for the watchlist UI is computed
  elsewhere from a session reference price; this property is the instantaneous
  move that drives the flash animation.
- **`timestamp` is always Unix seconds.** The Massive backend converts the SDK's
  millisecond timestamps before constructing a cache entry, so every consumer can
  assume seconds.
- **Division-by-zero guard:** a first-ever update has `previous_price == price`
  (see the cache), so `change_percent` is `0.0` and `direction` is `"flat"` —
  no spurious flash on initial render.

---

## 5. Price Cache — `cache.py`

The decoupling point and single source of truth for "what is the price right now."
It is **thread-safe** because the Massive backend writes from a worker thread
(`asyncio.to_thread`) while SSE and portfolio code read from the event loop.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price
        (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Design notes

- **`previous_price` is computed inside the cache**, not by the data source. Each
  `update()` carries the prior cached price forward, so `change`/`direction` are
  always correct regardless of who is writing. The source only supplies an
  absolute price.
- **Rounding to cents happens here**, in one place, so both backends store
  consistent precision. `change`/`change_percent` round to 4 dp for sub-cent
  fidelity on the derived values.
- **The `version` counter is the SSE change-detection mechanism.** It increments
  on every `update()`. The SSE generator only serializes and pushes when the
  version moved since its last tick — no redundant identical frames. (Note: it is
  read without the lock as a cheap monotonic int; a stale read just defers one
  push by one interval, which is harmless.)
- **`get_all()` returns a shallow copy** taken under the lock, so the SSE consumer
  iterates a stable snapshot while writers continue mutating the live dict. The
  contained `PriceUpdate`s are frozen, so sharing them is safe.
- **`remove()` exists so a de-watchlisted ticker stops streaming immediately.**
  It is called by each source's `remove_ticker()`.

---

## 6. Abstract Interface — `interface.py`

The contract every backend must satisfy. It defines a small lifecycle: start,
mutate the active set, stop.

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Why these signatures

- **`start`/`stop` are async.** Both backends own asyncio tasks; the Massive
  backend additionally awaits an immediate first fetch in `start` so the cache is
  warm before the first SSE event fires.
- **`add_ticker`/`remove_ticker` are async** even though the simulator's work is
  synchronous. It keeps callers uniform and leaves room for a future backend that
  must make a network call to subscribe/unsubscribe.
- **`get_tickers` is sync** — a cheap in-memory read with no I/O.
- **`remove_ticker` owns cache eviction** so a removed watchlist entry stops
  appearing in the stream on the next tick.

---

## 7. Seed Prices & Parameters — `seed_prices.py`

All the tunable "domain knowledge" lives here so the simulation logic stays pure.
Prices and per-ticker `μ`/`σ` are editable without touching math.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00, "TSLA": 250.00,
    "NVDA": 800.00, "META": 500.00, "JPM": 195.00, "V": 280.00, "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

### Design notes

- **Unknown tickers never break the sim.** A symbol absent from `SEED_PRICES` gets
  a random start price in `[50, 300]`; one absent from `TICKER_PARAMS` gets a copy
  of `DEFAULT_PARAMS`. This matters because users (and the AI) can add arbitrary
  tickers to the watchlist at runtime.
- **`DEFAULT_PARAMS` is copied, not shared.** The simulator does `dict(DEFAULT_PARAMS)`
  per ticker so two ad-hoc tickers don't alias the same params dict.
- **Correlation is sector-driven** and intentionally moderate (0.3–0.6) to keep
  the correlation matrix positive-definite (a hard requirement for Cholesky — see
  §8). `TSLA` is deliberately decorrelated (0.3 with everything) for visual
  variety, even though it lives in the `tech` set for grouping purposes.

---

## 8. GBM Simulator — `simulator.py`

Two classes: `GBMSimulator` is pure price math; `SimulatorDataSource` adapts it to
the `MarketDataSource` lifecycle and the cache.

### 8.1 The math: Geometric Brownian Motion

Each ticker evolves by the discrete GBM step:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt  +  σ·√dt·Z )
```

`exp(...)` keeps prices strictly positive; the `−σ²/2` Itô correction makes `μ`
the true expected log-return. `Z` is a standard-normal draw, **correlated across
tickers** (§8.3).

**Choosing `dt`.** Ticks fire every 500 ms, expressed as a fraction of a *trading*
year so the annualized `μ`/`σ` mean what they say:

```
TRADING_SECONDS_PER_YEAR = 252 days × 6.5 h × 3600 s = 5,896,800
DEFAULT_DT = 0.5 / 5,896,800 ≈ 8.48e-8
```

This tiny `dt` yields sub-cent per-tick jitter that accumulates into realistic
intraday wander over a session.

### 8.2 `GBMSimulator`

```python
class GBMSimulator:
    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        # Per-ticker state
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}

        # Cholesky decomposition of the correlation matrix (correlated moves)
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()
```

### 8.3 Correlation via Cholesky decomposition

Independent draws would make each stock wander alone. To make sectors move
together: build an N×N correlation matrix `C` from sector membership, take its
Cholesky factor `L` (`L·Lᵀ = C`), and each tick form `z_corr = L·z` from
independent `z ~ N(0, I)`. Component `i` of `z_corr` is the `Z` for ticker `i`.

```python
    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping.

          - Same tech sector:    0.6
          - Same finance sector: 0.5
          - TSLA with anything:  0.3 (it does its own thing)
          - Cross-sector:        0.3
          - Unknown tickers:     0.3
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in the tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR

        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR

        return CROSS_GROUP_CORR
```

> **Positive-definiteness is a hard requirement.** `np.linalg.cholesky` raises
> `LinAlgError` on a non-PD matrix. The chosen ρ values (0.3–0.6 off-diagonal,
> 1.0 diagonal) keep `C` well-conditioned. Pushing ρ much higher across many
> tickers risks a non-PD matrix — tune with care.

The matrix is rebuilt only on watchlist edits (add/remove), never per tick, so the
O(n²) cost is negligible.

### 8.4 The step function (hot path)

```python
    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # n independent standard normal draws
        z_independent = np.random.standard_normal(n)

        # Apply Cholesky to get correlated draws
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            # GBM step
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec, expect an event ~every 50 seconds
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker, shock_magnitude * 100,
                    "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result
```

One vectorized normal draw + one matrix-vector product per tick, then a cheap
scalar update per ticker. Comfortably handles dozens of tickers at 2 Hz.

### 8.5 Add / remove / accessors

```python
    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch init)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))
```

`_add_ticker_internal` defers the (expensive) Cholesky rebuild so the constructor
can add N tickers and rebuild once, while the public `add_ticker` rebuilds eagerly.

### 8.6 `SimulatorDataSource` — lifecycle wrapper

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            # Seed cache immediately so the ticker has a price right away
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Key behaviors:

- **Seed-on-start and seed-on-add** push prices to the cache immediately, so a
  newly-added ticker has a price the instant it's added — not 500 ms later.
- **The loop never dies**: `try/except` around `step()` logs and continues,
  matching the resilience contract the Massive backend follows.
- **`stop()` is idempotent**: it cancels the task and swallows `CancelledError`;
  safe to call twice (e.g. double shutdown).

---

## 9. Massive API Client — `massive_client.py`

The real-data backend. It polls the **Full Market Snapshot** endpoint — one call
covers the entire watchlist, so request count is independent of watchlist size and
stays under the free tier's 5 req/min at the default 15 s interval.

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers), self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"), e,
                    )
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop retries on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Design notes

- **Two-level error handling.** The outer `try/except` catches batch failures
  (auth/rate-limit/network) and keeps the loop alive. An inner `try/except` per
  snapshot skips a single malformed record (missing `last_trade`) without dropping
  the whole batch. This is the resilience contract from `MASSIVE_API.md`.
- **Synchronous SDK off the event loop.** `RESTClient` is blocking, so the actual
  HTTP call is wrapped in `asyncio.to_thread`. The event loop — serving SSE — never
  stalls during a poll.
- **Immediate warm-up.** `start()` awaits one `_poll_once()` before spawning the
  loop, so the cache holds real prices before the first SSE frame. The loop then
  sleeps *first* (`await asyncio.sleep` at the top) so it doesn't double-poll at t=0.
- **Timestamp conversion.** The SDK's `last_trade.timestamp` is treated as
  milliseconds and divided by 1000 to match the cache's Unix-seconds contract. (A
  price timestamp ~1000× in the future is the tell-tale sign the divisor needs
  adjusting for a given plan's payload — see `MASSIVE_API.md`.)
- **Ticker normalization** (`.upper().strip()`) on add/remove guards against
  case/whitespace mismatches between the watchlist UI and the snapshot response.
- **`add_ticker` is lazy** — it just appends; the ticker appears on the next poll.
  There's no per-ticker subscribe call, because the snapshot endpoint takes the
  whole list each time.

---

## 10. Factory — `factory.py`

The single place that knows about both concrete classes. Selection mirrors the
PLAN exactly.

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

The `.strip()` is load-bearing: a present-but-blank `MASSIVE_API_KEY=` in `.env`
falls through to the simulator instead of constructing a client with an empty key.
The returned source is **unstarted** — the caller owns `start()`/`stop()` so the
factory has no lifecycle side effects (and stays trivially unit-testable by
patching the env var).

---

## 11. SSE Streaming Endpoint — `stream.py`

A factory that builds a FastAPI router bound to a specific `PriceCache`, exposing
`GET /api/stream/prices` as an `EventSource`-compatible stream.

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    """

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Includes a retry directive so the browser auto-reconnects.
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices every `interval` seconds. Stops when the client
    disconnects (detected via request.is_disconnected()).
    """
    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()

                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    payload = json.dumps(data)
                    yield f"data: {payload}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Design notes

- **Version-gated pushes.** The generator compares `price_cache.version` to the
  last value it sent. If unchanged, it skips serialization entirely — no redundant
  frames on a quiet tick. With either backend writing continuously, the version
  effectively always changes, but the guard makes the endpoint correct (and cheap)
  even when the producer is idle.
- **Full-snapshot payload.** Each frame is a JSON object keyed by ticker
  (`{ticker: PriceUpdate.to_dict()}`), so a client that connects mid-session gets
  the complete current state on the first frame — sparklines and tables hydrate
  immediately rather than waiting for each ticker to tick.
- **Disconnect detection.** `request.is_disconnected()` is polled each loop so a
  closed browser tab tears the generator down promptly instead of leaking a task.
- **Reconnection is free.** The leading `retry: 1000` directive plus native
  `EventSource` retry means the browser reconnects ~1 s after any drop, with no
  client code. On reconnect it immediately receives the full snapshot.
- **Proxy-friendly headers.** `Cache-Control: no-cache` and `X-Accel-Buffering: no`
  prevent intermediaries (nginx) from buffering the stream.
- **Module-level `router`.** Note the `APIRouter` is created at module scope and
  the route is registered inside the factory; call `create_stream_router` exactly
  once per process to avoid duplicate route registration.

### Frontend consumption (illustrative)

```javascript
const es = new EventSource("/api/stream/prices");
es.onmessage = (e) => {
  const prices = JSON.parse(e.data);           // { AAPL: {price, direction, ...}, ... }
  for (const [ticker, u] of Object.entries(prices)) {
    applyFlash(ticker, u.direction);            // "up" → green, "down" → red
    updateSparkline(ticker, u.price);
  }
};
// EventSource auto-reconnects using the server's `retry: 1000` directive.
```

---

## 12. Public Surface — `__init__.py`

The import contract. Consumers depend only on these five names plus the factory;
the concrete sources and `GBMSimulator` are implementation detail.

```python
"""Market data subsystem for FinAlly."""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

---

## 13. FastAPI Lifecycle Integration

The market subsystem is owned by the FastAPI app: one cache, one source, started
on app startup and stopped on shutdown. The modern `lifespan` form is preferred
over deprecated `@app.on_event`.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    cache = PriceCache()
    source = create_market_data_source(cache)        # reads MASSIVE_API_KEY
    await source.start(load_watchlist_tickers())     # from the watchlist table

    # Stash on app.state so request handlers and the watchlist routes can reach them
    app.state.price_cache = cache
    app.state.market_source = source

    # Mount the SSE route (bound to this cache)
    app.include_router(create_stream_router(cache))

    try:
        yield
    finally:
        # --- shutdown ---
        await source.stop()


app = FastAPI(lifespan=lifespan)
```

- **One cache, one source per process.** Stashing them on `app.state` gives the
  trade and watchlist routes a handle without module globals.
- **Warm cache before first request.** Because `start()` seeds (simulator) or
  polls once (Massive) synchronously, the first SSE client and the first
  `/api/portfolio` call already see prices.
- **Graceful shutdown.** `source.stop()` in the `finally` cancels the background
  task cleanly, so reloads and container stops don't leak tasks.

> The PLAN sketches this with the older `@app.on_event("startup"/"shutdown")`
> decorators; either works, but `lifespan` is the non-deprecated path and keeps
> startup/shutdown symmetric in one place.

---

## 14. Watchlist Coordination

The watchlist is the contract between the user/AI and the data source: the source
should track exactly the tickers on the watchlist. The watchlist routes mutate the
**same** `source` instance held on `app.state`.

```python
# POST /api/watchlist  {"ticker": "PYPL"}
async def add_to_watchlist(ticker: str, request: Request):
    ticker = ticker.upper().strip()
    db_add_watchlist_row(ticker)                       # persist
    await request.app.state.market_source.add_ticker(ticker)   # start streaming
    return {"ticker": ticker}

# DELETE /api/watchlist/{ticker}
async def remove_from_watchlist(ticker: str, request: Request):
    ticker = ticker.upper().strip()
    db_remove_watchlist_row(ticker)                    # persist
    await request.app.state.market_source.remove_ticker(ticker)  # stop + evict
    return {"ticker": ticker}
```

- **Add** → source begins producing for the ticker (simulator: seeded immediately
  and included next step; Massive: appears on the next poll). The new ticker shows
  up in subsequent SSE frames automatically.
- **Remove** → source drops it *and evicts the cache entry*, so it disappears from
  the stream on the next tick.
- **Trade execution and portfolio valuation** read `cache.get_price(ticker)` for
  fill prices and mark-to-market. They are entirely agnostic to which backend is
  running — this is the payoff of the producer/consumer split.

```python
# Trade fill price lookup (no knowledge of the backend)
price = request.app.state.price_cache.get_price(ticker)
if price is None:
    raise HTTPException(400, f"No live price for {ticker}")
fill_value = price * quantity
```

---

## 15. Error Handling & Edge Cases

| Case | Handling |
|------|----------|
| **Simulator step raises** (numerical / state) | `try/except` in `_run_loop` logs via `logger.exception` and continues; the loop never dies. |
| **Massive batch failure** (401/429/network/timeout) | Outer `try/except` in `_poll_once` logs and returns; retried next interval. Loop survives. |
| **Malformed snapshot** (missing `last_trade`) | Inner `try/except (AttributeError, TypeError)` skips that one ticker, keeps the rest of the batch. |
| **Unknown ticker added to simulator** | Random seed price in `[50, 300]` + `DEFAULT_PARAMS`; never raises. |
| **First update for a ticker** | Cache sets `previous_price == price` → `direction='flat'`, `change_percent=0.0`; no spurious flash. |
| **Empty watchlist** | `step()` returns `{}`; Massive `_poll_once` early-returns on empty `_tickers`. SSE sends nothing until prices exist. |
| **SSE client disconnect** | `request.is_disconnected()` breaks the loop; `CancelledError` is caught and logged. No leaked tasks. |
| **Quiet tick (no price change)** | Version unchanged → SSE skips the frame. |
| **Non-PD correlation matrix** | Prevented by construction (moderate ρ). Would surface immediately as `LinAlgError` on a watchlist edit if ρ values were mistuned. |
| **`stop()` called twice** | Guarded by `self._task.done()`; safe and idempotent. |
| **Blank `MASSIVE_API_KEY=`** | `.strip()` in the factory → falls through to the simulator. |

---

## 16. Testing Strategy

The subsystem is covered by **73 unit tests** across 6 modules in
`backend/tests/market/` (overall 84% coverage). The design makes this cheap because
everything is offline-testable.

| Concern | What to assert |
|---------|----------------|
| **`PriceUpdate`** | `change`/`change_percent`/`direction` correctness; div-by-zero guard; `to_dict()` keys; frozenness. |
| **`PriceCache`** | first-update flat behavior; previous_price carry-forward; version increments; `remove`/`get_all` copy semantics; thread-safety under concurrent writers. |
| **GBM math** | prices stay positive & finite over many steps; determinism with seeded `random`/`np.random`; `event_probability=1.0` forces a shock every step. |
| **Correlation** | `L·Lᵀ ≈ C`; over many steps, tech-pair return correlation ≈ configured ρ within tolerance; matrix dimension tracks ticker count. |
| **Add/remove** | removed tickers vanish from `get_tickers()` and the cache; Cholesky rebuilds; unknown ticker gets defaults. |
| **`SimulatorDataSource`** | start seeds cache; loop writes; add seeds immediately; remove evicts; `stop()` cancels cleanly and is idempotent. |
| **`MassiveDataSource`** | `_poll_once` writes parsed snapshots (SDK mocked); malformed record skipped; batch exception swallowed; ms→s timestamp; `add/remove` normalize case. |
| **Factory** | key set → `MassiveDataSource`; unset/blank → `SimulatorDataSource` (patch `os.environ`). |
| **SSE** | version-gated emission; full-snapshot payload shape; disconnect ends the generator. |

Example — deterministic GBM test:

```python
def test_gbm_prices_stay_positive_and_finite():
    random.seed(42); np.random.seed(42)
    sim = GBMSimulator(["AAPL", "MSFT", "JPM"])
    for _ in range(10_000):
        prices = sim.step()
        assert all(p > 0 and math.isfinite(p) for p in prices.values())

def test_cholesky_reconstructs_correlation():
    sim = GBMSimulator(["AAPL", "MSFT", "JPM", "V"])
    L = sim._cholesky
    assert np.allclose(L @ L.T, build_expected_corr(sim.get_tickers()))
```

E2E tests run against the simulator by default (no key, no network), which is
exactly why the simulator is the fallback backend.

---

## 17. Configuration Summary

| Knob | Where | Default | Effect |
|------|-------|---------|--------|
| `MASSIVE_API_KEY` | env / `.env` | unset | Set & non-empty → Massive backend; else simulator. |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Snapshot poll cadence (free-tier safe at 15 s). |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Simulator tick cadence / flash rate. |
| `event_probability` | `GBMSimulator` / `SimulatorDataSource` | `0.001` | Per-tick-per-ticker chance of a 2–5% shock. |
| `dt` | `GBMSimulator` | `~8.48e-8` | Time step; larger → bigger per-tick moves. |
| `SEED_PRICES` / `TICKER_PARAMS` | `seed_prices.py` | per ticker | Start price and `μ`/`σ` per ticker. |
| `CORRELATION_GROUPS` + ρ constants | `seed_prices.py` | tech 0.6 / fin 0.5 / cross 0.3 | Which sectors move together. |
| SSE `interval` | `_generate_events` | `0.5` s | How often the stream checks the version and pushes. |
| SSE `retry` | `_generate_events` | `1000` ms | Browser reconnect delay after a drop. |

---

### Summary

The market data backend is a **producer/consumer system around one thread-safe
cache**. Two interchangeable producers — a numpy GBM simulator and a Massive REST
poller — implement a single five-method ABC and are selected by one environment
variable in a factory. Every consumer (SSE, portfolio, trades) reads only the
cache and is oblivious to the source. Background loops are individually resilient,
the SSE endpoint is version-gated and reconnect-friendly, and the whole subsystem
is offline-testable — which is what lets the project run, and its E2E suite pass,
with zero external dependencies.
</content>
</invoke>
