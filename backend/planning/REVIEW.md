# Review: Changes Since Last Commit

## Findings

### High: Project-local Stop hook runs Codex after every Claude turn

- File: `.claude/settings.json:7`

The new `Stop` hook runs:

```bash
codex exec "Review changes since last commit and write results to a file named planning/REVIEW.md"
```

Because this is committed project configuration, anyone using Claude in this repo would trigger an extra Codex process after every Claude response. That creates unexpected model cost and latency, and it repeatedly rewrites the same review file. It is also surprising behavior for collaborators because opening the repo in an interactive agent session starts an automatic secondary agent workflow.

Recommendation: remove this hook before committing. If automatic review generation is desired, make it explicit through a script, CI job, or manually invoked command, and write timestamped output instead of repeatedly overwriting `planning/REVIEW.md`.

### Low: README still presents `OPENROUTER_API_KEY` as globally required

- File: `README.md:49`

The updated Quick Start now correctly says the currently runnable path is the market-data terminal demo:

```bash
cd backend
uv sync
uv run market_data_demo.py
```

That demo does not require `OPENROUTER_API_KEY`, but the environment table still marks the key as required without scoping it to the planned AI chat/full app. A new user can reasonably read this as a prerequisite for the current Quick Start.

Recommendation: mark `OPENROUTER_API_KEY` as required for AI chat/full app only, or split the table into current demo requirements and planned full-app requirements.

### Low: Duplicate review artifacts are accumulating

- File: `planning/REVIEW4.md` at the repository root, untracked
- File: `backend/planning/REVIEW.md`, untracked

The diff now has multiple review outputs in different `planning/` directories. This makes it unclear which review is canonical and increases the chance that later automated runs overwrite or orphan useful review history.

Recommendation: choose one review location and naming convention. If reviews are meant to be preserved, prefer timestamped or numbered files created by an explicit command.

## Notes

- `README.md` otherwise improves accuracy by clearly labeling the project as in development and the feature list as target functionality.
- `planning/PLAN.md` only changed by removing the trailing newline at EOF. There is no behavioral impact, but restoring the newline would avoid needless diff noise.
- No runtime code changed in this diff, so I did not run the backend test suite.
