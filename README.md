# FinAlly — AI Trading Workstation

A visually stunning AI-powered trading workstation that streams live market data, simulates portfolio trading, and integrates an LLM chat assistant that can analyze positions and execute trades via natural language.

Built entirely by coding agents as a capstone project for an agentic AI coding course.

## Status

🚧 **In development.** The market data component (GBM simulator + price cache) is complete — see [`planning/MARKET_DATA_SUMMARY.md`](planning/MARKET_DATA_SUMMARY.md). The remainder of the platform (portfolio, API, AI chat, frontend, Docker) is still being built per [`planning/PLAN.md`](planning/PLAN.md).

## Features (target)

- **Live price streaming** via SSE with green/red flash animations
- **Simulated portfolio** — $10k virtual cash, market orders, instant fills
- **Portfolio visualizations** — heatmap (treemap), P&L chart, positions table
- **AI chat assistant** — analyzes holdings, suggests and auto-executes trades
- **Watchlist management** — track tickers manually or via AI
- **Dark terminal aesthetic** — Bloomberg-inspired, data-dense layout

## Architecture

Single Docker container serving everything on port 8000:

- **Frontend**: Next.js (static export) with TypeScript and Tailwind CSS
- **Backend**: FastAPI (Python/uv) with SSE streaming
- **Database**: SQLite with lazy initialization
- **AI**: LiteLLM → OpenRouter (Cerebras inference) with structured outputs
- **Market data**: Built-in GBM simulator (default) or Massive API (optional)

## Quick Start

The market data simulator runs today as a live terminal demo:

```bash
cd backend
uv sync
uv run market_data_demo.py
```

Once the full app is containerized, it will run with:

```bash
cp .env.example .env          # add your OPENROUTER_API_KEY
docker build -t finally .
docker run -v finally-data:/app/db -p 8000:8000 --env-file .env finally
# open http://localhost:8000
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `OPENROUTER_API_KEY` | Yes | OpenRouter API key for AI chat |
| `MASSIVE_API_KEY` | No | Massive (Polygon.io) key for real market data; omit to use simulator |
| `LLM_MOCK` | No | Set `true` for deterministic mock LLM responses (testing) |

## Project Structure

```
finally/
├── frontend/    # Next.js static export (planned)
├── backend/     # FastAPI uv project (market data complete)
├── planning/    # Project documentation and agent contracts
├── test/        # Playwright E2E tests (planned)
├── db/          # SQLite volume mount (runtime)
└── scripts/     # Start/stop helpers (planned)
```

## License

See [LICENSE](LICENSE).
