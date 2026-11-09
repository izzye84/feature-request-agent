# Dagster Agentic Orchestration Demo

A feature request goes in; a reviewed pull request comes out.

1. **Grok (xAI API)** turns a plain-English request into a structured spec and a task plan.
2. **A Cursor SDK cloud agent** implements the plan in this repo and opens a real pull request.
3. **A second Cursor SDK cloud agent**, run by a GitHub Action, reviews the PR. `review_policy.py` then auto-approves simple, secret-free changes and routes everything else to a human. See [`pr_review_gate/`](pr_review_gate/README.md).

Dagster orchestrates steps 1–2 as assets.

## Pipeline overview

```
ingest_requirements → analyze_and_plan → generate_code ──▶ PR ──▶ PR review gate (GitHub Action)
```

| Asset | Agent role | Model |
|---|---|---|
| `ingest_requirements` | Parses raw feature request → structured JSON spec | Grok |
| `analyze_and_plan` | Decomposes spec → ordered task plan | Grok |
| `generate_code` | Cursor cloud agent implements the plan and opens a real PR | Cursor |

A sensor, **`new_feature_request_sensor`**, polls `requests/queue.json` for new requests and triggers runs automatically.

---

## Setup

### 1. Install dependencies

```bash
uv sync
```

### 2. Set API keys

Create a `.env` file in the project root:

```bash
XAI_API_KEY=xai-...
CURSOR_API_KEY=cursor_...
```

`CURSOR_API_KEY` is used by `generate_code` (and to run the [PR review gate](pr_review_gate/README.md) locally; in CI it's a repository secret). Cursor's GitHub App must have access to the repo: https://cursor.com/dashboard/integrations. The repo is set by `REPO` in `definitions.py`.

Every run opens a real pull request, which then goes through the PR review gate.

The Cursor agent follows Dagster's `dagster-expert` skill, committed at `.cursor/skills/dagster-expert/`. Cursor cloud agents load project skills from the repo they clone, and the `generate_code` prompt tells the agent to use it. It's copied from [dagster-io/skills](https://github.com/dagster-io/skills) (`release-stable`, 1.13.29; Apache-2.0). To update it, copy `plugins/dagster/skills/dagster-expert/` from that repo again.

To get an API key: go to **console.x.ai** → API Keys → Create Key.

> **Note:** The xAI API is billed per token, separately from any Grok / X Premium subscription.

### 3. Start the Dagster UI

```bash
uv run dagster dev
```

Open http://localhost:3000 in your browser.

---

## Running the pipeline

### Option 1: Materialize all assets (recommended for demo)

1. Open http://localhost:3000
2. Click **Assets** in the top nav
3. Select all assets in the `agentic_pipeline` group (or click **Materialize all**)
4. The **Launchpad** will open showing the runtime config — see below

### Option 2: CLI launch

```bash
uv run dg launch --assets "ingest_requirements,analyze_and_plan,generate_code"
```

---

## Runtime configuration

All config has sensible defaults — **you can run the pipeline with zero changes** and it will use the default feature request. To customise, edit the config in the Launchpad before launching.

```yaml
ops:
  ingest_requirements:
    config:
      feature_request: "Add a data quality check that validates transaction amounts
        are within ±3 standard deviations of the rolling 30-day mean before writing
        to the curated layer."
      model: "grok-4.7"
      max_tokens: 4096
```

### Config fields

| Field | Default | Description |
|---|---|---|
| `feature_request` | *(data quality check example)* | The feature request text. Paste any plain-English request here. |
| `model` | `grok-4.7` | Any xAI model string. See model options below. |
| `max_tokens` | `4096` | Max tokens per Grok response. Increase for complex codegen. |

### Available Grok models

The default is `grok-4.7`. See the [xAI models page](https://docs.x.ai/developers/models) for the current list of model names, context windows, and pricing.

---

## What to look for in the UI

After a successful run:

- **Asset graph** — three nodes (Grok, Grok, Cursor) showing the dependency chain
- **`ingest_requirements` metadata** — structured JSON spec, complexity, estimated hours, tokens used
- **`analyze_and_plan` metadata** — full task plan JSON, task count, risk list, reviewer notes
- **`generate_code` metadata** — PR link, branch, the Cursor agent's summary, and run duration
- **The PR on GitHub** — the review gate's comment, plus an approval or the `needs-human-review` label

---

## Demoing the sensor

The sensor is **STOPPED by default**. Enable it in the Dagster UI under **Automation → Sensors** before running this demo.

### `new_feature_request_sensor` (event-driven trigger)

The sensor polls `requests/queue.json` every 30 seconds for new entries. An example entry is already in the file — here's how to demo it:

1. Enable `new_feature_request_sensor` in the Automation tab
2. Open `requests/queue.json` and add a new entry with a unique `id`:

```json
[
  {
    "id": "FR-001",
    "feature_request": "...",
    "model": "grok-4.7"
  },
  {
    "id": "FR-002",
    "feature_request": "Write a Dagster asset that reads a list of stock tickers and returns the top 5 by 30-day price volatility.",
    "model": "grok-4.7"
  }
]
```

3. Save the file and wait up to 30 seconds
4. Watch a new run appear automatically in the **Runs** tab

The sensor cursor persists processed IDs between ticks — adding `FR-002` again will not trigger a duplicate run.

**In production:** replace the file read in `sensors.py` with a Jira/ServiceNow/S3 API call. The sensor logic (cursor, deduplication, `RunRequest` construction) stays identical.

---

## PR review gate (Cursor SDK + GitHub Actions)

A separate prototype in [`pr_review_gate/`](pr_review_gate/README.md): a GitHub Action runs a Cursor SDK cloud agent on every pull request to this repo, auto-approves only simple, secret-free changes, and routes everything else to a human. It's its own uv project and doesn't depend on the Dagster code.

---

## Project structure

```
.cursor/skills/
└── dagster-expert/         # Dagster's agent skill, loaded by the Cursor cloud agent
src/dagster_agent_demo/
├── definitions.py          # Dagster entry point — binds resources, loads defs
├── resources/
│   ├── cursor.py           # CursorResource: runs a Cursor cloud agent that opens a PR
│   └── grok.py             # Custom GrokResource (modelled on dagster-anthropic)
└── defs/
    ├── __init__.py         # Empty — load_from_defs_folder handles discovery
    ├── assets.py           # Feature-request pipeline (Grok → Grok → Cursor)
    └── sensors.py          # New-request sensor
```

---

## Learn more

- [Dagster Documentation](https://docs.dagster.io/)
- [xAI API docs](https://docs.x.ai/overview)
- [dagster-anthropic integration](https://docs.dagster.io/integrations/libraries/anthropic) — the design `GrokResource` follows
- [Dagster University](https://courses.dagster.io/)
