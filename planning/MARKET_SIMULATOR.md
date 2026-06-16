# Market Simulator — Approach & Code Structure

The default market data backend, used whenever `MASSIVE_API_KEY` is **not** set. It
generates realistic, correlated, continuously-moving stock prices entirely
in-process — no network, no API key, no rate limits. It implements the same
`MarketDataSource` interface as the Massive backend (see `MARKET_INTERFACE.md`), so
the rest of the app cannot tell the difference.

---

## 1. Why simulate, and what "realistic" means here

The simulator exists so the project runs out-of-the-box for anyone without a Massive
key, and so E2E tests are fast, free, and deterministic-ish. To feel real on a
trading terminal, the price stream needs four properties:

1. **Continuous small moves** — sub-cent-to-cent jitter every tick, not random jumps.
2. **Plausible drift & volatility per ticker** — TSLA should be jumpier than V.
3. **Correlation** — tech names move together; a sector ripples, not just one stock.
4. **Occasional drama** — rare sudden 2–5% moves to make the watchlist flash.

Geometric Brownian Motion (GBM) gives us 1–3 for free; a small random "event" knob
adds 4.

---

## 2. The math: Geometric Brownian Motion

Each ticker's price evolves by the discrete GBM step:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt  +  σ·√dt·Z )
```

| Symbol | Meaning |
|--------|---------|
| `S(t)` | current price |
| `μ`    | annualized drift (expected return) |
| `σ`    | annualized volatility |
| `dt`   | time step as a fraction of a **trading** year |
| `Z`    | a standard-normal draw (correlated across tickers — see §4) |

GBM guarantees prices stay positive (it's exp of a normal), and the `−σ²/2` term is
the Itô correction so that `μ` is the true expected log-return.

### Choosing `dt`

Ticks fire every 500 ms. We express that as a fraction of a trading year so that the
annualized `μ`/`σ` parameters mean what they say:

```
TRADING_SECONDS_PER_YEAR = 252 days × 6.5 hours × 3600 s = 5,896,800
DEFAULT_DT = 0.5 / 5,896,800 ≈ 8.48e-8
```

This tiny `dt` produces sub-cent moves per tick that accumulate naturally over
minutes — exactly the gentle flicker a terminal should show, with realistic
intraday wander over a session.

```python
class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600   # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8

    def __init__(self, tickers, dt=DEFAULT_DT, event_probability=0.001):
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}   # {ticker: {"mu":..,"sigma":..}}
        self._cholesky = None                            # correlation factor (see §4)
        for t in tickers:
            self._add_ticker_internal(t)
        self._rebuild_cholesky()
```

---

## 3. Seed prices & per-ticker parameters

Starting prices and `μ`/`σ` live in `seed_prices.py` so they're easy to tune without
touching simulation logic. Values are realistic-as-of-project-creation.

```python
SEED_PRICES = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00, "TSLA": 250.00,
    "NVDA": 800.00, "META": 500.00, "JPM": 195.00,  "V": 280.00,   "NFLX": 600.00,
}

# sigma = annualized volatility (higher → jumpier); mu = annualized drift
TICKER_PARAMS = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},   # very jumpy
    "NVDA": {"sigma": 0.40, "mu": 0.08},   # jumpy + strong upward drift
    "JPM":  {"sigma": 0.18, "mu": 0.04},   # calm (bank)
    "V":    {"sigma": 0.17, "mu": 0.04},   # calm (payments)
    # ... GOOGL, AMZN, META, NFLX
}

DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}   # for ad-hoc tickers added at runtime
```

Tickers added at runtime that aren't in the seed map get `DEFAULT_PARAMS` and a
random start price (`random.uniform(50, 300)`), so the simulation never fails on an
unknown symbol.

---

## 4. Correlation via Cholesky decomposition

Independent random draws would make every stock wander on its own — unrealistic. We
want tech names to move together. The standard technique:

1. Build an **N×N correlation matrix** `C` from sector membership.
2. Compute its **Cholesky factor** `L` (lower-triangular, `L·Lᵀ = C`).
3. Each tick, draw `z ~ N(0, I)` (independent) and form `z_corr = L·z`. The vector
   `z_corr` now has the desired correlation structure; feed component `i` as the `Z`
   for ticker `i`.

```python
def _rebuild_cholesky(self):
    n = len(self._tickers)
    if n <= 1:
        self._cholesky = None
        return
    corr = np.eye(n)
    for i in range(n):
        for j in range(i + 1, n):
            rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
            corr[i, j] = corr[j, i] = rho
    self._cholesky = np.linalg.cholesky(corr)
```

### Correlation rules

```python
CORRELATION_GROUPS = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR    = 0.6   # tech moves together
INTRA_FINANCE_CORR = 0.5   # finance moves together
CROSS_GROUP_CORR   = 0.3   # between sectors / unknown tickers
TSLA_CORR          = 0.3   # TSLA "does its own thing" even though it's tech
```

The matrix is rebuilt whenever tickers are added/removed. It's O(n²), but n stays
small (<50), so the cost is negligible and only paid on watchlist edits, not per tick.

> **Why Cholesky must succeed:** the correlation matrix has to be positive-definite
> or `np.linalg.cholesky` raises. The chosen ρ values (0.3–0.6 off-diagonal, 1.0 on
> the diagonal) keep it well-conditioned. If you tune ρ much higher across many
> tickers you risk a non-PD matrix.

---

## 5. The step function (hot path)

Called every 500 ms. Advances every ticker one GBM step, applies rare events, and
returns the new prices rounded to cents.

```python
def step(self) -> dict[str, float]:
    n = len(self._tickers)
    if n == 0:
        return {}

    z = np.random.standard_normal(n)                 # independent draws
    z_corr = self._cholesky @ z if self._cholesky is not None else z

    result = {}
    for i, ticker in enumerate(self._tickers):
        p = self._params[ticker]
        mu, sigma = p["mu"], p["sigma"]

        drift     = (mu - 0.5 * sigma**2) * self._dt
        diffusion = sigma * math.sqrt(self._dt) * z_corr[i]
        self._prices[ticker] *= math.exp(drift + diffusion)

        # Rare "event": ~0.1% chance per tick per ticker.
        # With 10 tickers at 2 ticks/s, expect an event roughly every ~50s.
        if random.random() < self._event_prob:
            shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
            self._prices[ticker] *= (1 + shock)

        result[ticker] = round(self._prices[ticker], 2)
    return result
```

Performance: one vectorized normal draw + one matrix-vector product per tick, then a
cheap per-ticker scalar update. Easily handles dozens of tickers at 2 Hz.

---

## 6. Lifecycle wrapper: `SimulatorDataSource`

The `GBMSimulator` is pure math. `SimulatorDataSource` adapts it to the
`MarketDataSource` interface — owning the background loop and writing to the cache.

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache, update_interval=0.5, event_probability=0.001):
        ...

    async def start(self, tickers):
        self._sim = GBMSimulator(tickers, event_probability=self._event_prob)
        for t in tickers:                       # seed cache so SSE has data at t=0
            price = self._sim.get_price(t)
            if price is not None:
                self._cache.update(ticker=t, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def _run_loop(self):
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")   # survive, keep ticking
            await asyncio.sleep(self._interval)

    async def add_ticker(self, ticker):
        self._sim.add_ticker(ticker)            # rebuilds Cholesky
        price = self._sim.get_price(ticker)
        if price is not None:
            self._cache.update(ticker=ticker, price=price)   # immediate, don't wait a tick

    async def remove_ticker(self, ticker):
        self._sim.remove_ticker(ticker)         # rebuilds Cholesky
        self._cache.remove(ticker)

    async def stop(self):
        # cancel task, await CancelledError — idempotent
        ...
```

Notes:

- **Seed-on-start and seed-on-add**: prices are pushed to the cache immediately so a
  newly-added ticker has a price the instant it's added, not 500 ms later.
- **The loop never dies**: a `try/except` around `step()` logs and continues, matching
  the resilience contract the Massive backend follows.
- **Cache eviction on remove** keeps a de-watchlisted ticker from streaming.

---

## 7. File layout

```
backend/app/market/
├── interface.py     # MarketDataSource ABC (shared contract)
├── simulator.py     # GBMSimulator + SimulatorDataSource   ← this document
├── seed_prices.py   # SEED_PRICES, TICKER_PARAMS, correlation constants
├── massive_client.py# MassiveDataSource (real-data backend)
├── cache.py         # PriceCache
├── factory.py       # create_market_data_source() — picks backend from env
└── stream.py        # SSE endpoint reading the cache
```

---

## 8. Testing the simulator

The math and lifecycle are unit-testable without any clock or network:

- **GBM validity**: after many `step()` calls all prices stay positive and finite.
- **Determinism**: seed `random`/`np.random` → reproducible sequences for assertions.
- **Correlation**: over many steps, tech-pair return correlation ≈ the configured ρ
  (within tolerance); the Cholesky factor satisfies `L·Lᵀ ≈ C`.
- **Add/remove**: matrix dimension tracks ticker count; removed tickers vanish from
  `get_tickers()` and the cache.
- **Events**: with `event_probability=1.0`, every step applies a 2–5% shock.

These run fast and offline, which is why E2E tests default to the simulator.

---

## 9. Tuning cheat-sheet

| Want… | Change |
|-------|--------|
| Faster/slower flicker | `update_interval` (seconds per tick) |
| Bigger per-tick moves | per-ticker `sigma`, or `dt` |
| Stronger trend | per-ticker `mu` |
| More/less drama | `event_probability` (default 0.001) |
| Different sectors moving together | `CORRELATION_GROUPS` + the ρ constants |
| New default ticker | add to `SEED_PRICES` and `TICKER_PARAMS` |
