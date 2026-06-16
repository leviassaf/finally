# Review: Changes Since Last Commit

## Findings

### High: Massive SDK timestamp attribute is documented and tested incorrectly

`planning/MASSIVE_API.md:120`, `planning/MASSIVE_API.md:154`, `planning/MASSIVE_API.md:162`, and `planning/MARKET_INTERFACE.md:246` all direct future work toward `snap.last_trade.timestamp`. The implemented client uses the same attribute at `backend/app/market/massive_client.py:103`, and the tests mock that shape at `backend/tests/market/test_massive.py:11`.

However, the locked dependency is `massive==2.2.0` (`backend/uv.lock:269`), and local inspection of `massive.rest.models.trades.LastTrade.__annotations__` shows `sip_timestamp` is present while `timestamp` is not. Upstream `massive-com/client-python` maps snapshot `lastTrade.t` to `LastTrade.sip_timestamp`, not `timestamp`.

Impact: with a real Massive snapshot, every otherwise valid ticker will raise `AttributeError`, be treated as malformed, and be skipped. The poller will stay alive but the cache will never receive real prices. The docs reinforce the wrong production shape and the tests mask it by using mocks with the same wrong field.

Recommendation: update the client and docs to use `snap.last_trade.sip_timestamp`; add a test using a real `LastTrade`/`TickerSnapshot.from_dict(...)` model instead of a free-form `MagicMock`.

### Medium: Removed tickers are not pushed to existing SSE clients immediately

`planning/MARKET_INTERFACE.md:112` says removing a ticker stops streaming immediately, and `planning/MARKET_SIMULATOR.md:245` says cache eviction keeps a removed ticker from streaming. The code does remove the cache entry (`backend/app/market/cache.py:59`), but `PriceCache.remove()` does not increment `_version`; only `update()` does (`backend/app/market/cache.py:41`). The SSE generator only sends a frame when `price_cache.version` changes (`backend/app/market/stream.py:75`).

Impact: if a ticker is removed and no later price update occurs, connected EventSource clients are not notified about the deletion. In the Massive backend this can last until the next poll updates another ticker, and if the removed ticker was the only tracked symbol it may never be corrected. This is both a documentation accuracy issue and a likely behavior bug in the current market layer.

Recommendation: either bump `_version` when `remove()` actually deletes an entry, or change the docs to stop promising immediate streaming removal. Add a cache/SSE test for remove-driven version changes.

## Verification

- Reviewed untracked files:
  - `planning/MASSIVE_API.md`
  - `planning/MARKET_INTERFACE.md`
  - `planning/MARKET_SIMULATOR.md`
- Compared docs against `backend/app/market/*` and `backend/tests/market/*`.
- Checked official Massive docs/source:
  - Full Market Snapshot docs: `https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot`
  - Python SDK source: `https://github.com/massive-com/client-python`
- Ran local dependency inspection with `UV_CACHE_DIR=/tmp/uv-cache uv run python`; confirmed `LastTrade` has `sip_timestamp` and no `timestamp`.
- Attempted `UV_CACHE_DIR=/tmp/uv-cache uv run --extra dev pytest tests/market/test_massive.py tests/market/test_cache.py` from `backend/`. The run started and collected 26 tests, but did not complete within the tool timeout after the first test output. No full test result is available from this review pass.
