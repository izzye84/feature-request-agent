# Dagster Agentic Orchestration Demo

A Dagster pipeline that demonstrates agentic orchestration using Grok (xAI). A multi-step agent workflow that ingests a feature request, plans implementation tasks, generates code, runs quality gates, and produces a PR-ready artifact — all orchestrated as Dagster assets with full lineage, retries, and observability.

It also includes a **PR review gate**: a GitHub Action that runs a Cursor SDK cloud agent on every pull request to this repo, auto-approves only simple, secret-free changes, and routes everything else to a human. See [`pr_review_gate/`](pr_review_gate/README.md).

## Pipeline overview

```
ingest_requirements → analyze_and_plan → generate_code → run_quality_gates → publish_artifact
```

| Asset | Agent role | Grok call |
|---|---|---|
| `ingest_requirements` | Parses raw feature request → structured JSON spec | Yes |
| `analyze_and_plan` | Decomposes spec → ordered task plan | Yes |
| `generate_code` | Implements task plan → Python/SQL code | Yes |
| `run_quality_gates` | LLM code review + threshold guardrail | Yes |
| `publish_artifact` | Assembles PR description + lineage metadata | No |

Two sensors are also included:
- **`new_feature_request_sensor`** — polls for new requests and triggers runs automatically
- **`quality_gate_failure_sensor`** — feedback loop: auto-retries with a relaxed threshold when quality gates fail

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

`CURSOR_API_KEY` is only needed to run the [PR review gate](pr_review_gate/README.md) locally (in CI it's a repository secret).

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
uv run dg launch --assets "ingest_requirements,analyze_and_plan,generate_code,run_quality_gates,publish_artifact"
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
      quality_gate_threshold: 0.35
      reviewer: "data-eng-team"
```

### Config fields

| Field | Default | Description |
|---|---|---|
| `feature_request` | *(data quality check example)* | The feature request text. Paste any plain-English request here. |
| `model` | `grok-4.7` | Any xAI model string. See model options below. |
| `max_tokens` | `4096` | Max tokens per Grok response. Increase for complex codegen. |
| `quality_gate_threshold` | `0.35` | Float 0.0–1.0. LLM review score must meet or exceed this to pass. Raise it (e.g. `0.99`) to deliberately trigger a gate failure for demo purposes. |
| `reviewer` | `data-eng-team` | Label shown in the PR artifact. No functional effect. |

### Available Grok models

The default is `grok-4.7`. See the [xAI models page](https://docs.x.ai/developers/models) for the current list of model names, context windows, and pricing.

---

## What to look for in the UI

After a successful run:

- **Asset graph** — five nodes with Grok badges showing the dependency chain
- **`ingest_requirements` metadata** — structured JSON spec, complexity, estimated hours, tokens used
- **`analyze_and_plan` metadata** — full task plan JSON, task count, risk list, reviewer notes
- **`generate_code` metadata** — generated Python source code, line count, artifact URL
- **`run_quality_gates` metadata** — review JSON, overall score (0.0–1.0), security concerns, suggestions
- **`publish_artifact` metadata** — rendered markdown PR description ready to copy into GitHub

### Triggering the quality gate failure / feedback loop

Set `quality_gate_threshold` to `0.99` in the Launchpad config to force a gate failure. The `quality_gate_failure_sensor` (when enabled) will automatically re-submit the run with a lower threshold, demonstrating the feedback loop.

---

## Demoing the sensors

Both sensors are **STOPPED by default**. Enable them in the Dagster UI under **Automation → Sensors** before running these demos.

### Demo 1: `quality_gate_failure_sensor` (feedback loop)

1. Enable `quality_gate_failure_sensor` in the Automation tab
2. Launch a run with `quality_gate_threshold: 0.99` (forces a gate failure)
3. Watch the run fail at `run_quality_gates`
4. The sensor fires on the next tick and automatically re-submits the run with `quality_gate_threshold: 0.89`
5. Keep watching — it will keep lowering the threshold until the gate passes

This demonstrates the **agent reflects and retries** pattern: the pipeline self-corrects without human intervention.

### Demo 2: `new_feature_request_sensor` (event-driven trigger)

The sensor polls `requests/queue.json` every 30 seconds for new entries. An example entry is already in the file — here's how to demo it:

1. Enable `new_feature_request_sensor` in the Automation tab
2. Open `requests/queue.json` and add a new entry with a unique `id`:

```json
[
  {
    "id": "FR-001",
    "feature_request": "...",
    "model": "grok-4.7",
    "quality_gate_threshold": 0.35,
    "reviewer": "data-eng-team"
  },
  {
    "id": "FR-002",
    "feature_request": "Write a Dagster asset that reads a list of stock tickers and returns the top 5 by 30-day price volatility.",
    "model": "grok-4.7",
    "quality_gate_threshold": 0.35,
    "reviewer": "quant-team"
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
src/dagster_agent_demo/
├── definitions.py          # Dagster entry point — binds GrokResource, loads defs
├── resources/
│   └── grok.py             # Custom GrokResource (modelled on dagster-anthropic)
└── defs/
    ├── __init__.py         # Empty — load_from_defs_folder handles discovery
    ├── assets.py           # Grok feature-request pipeline
    └── sensors.py          # Feedback-loop sensor + new-request sensor
```

---

## Learn more

- [Dagster Documentation](https://docs.dagster.io/)
- [xAI API docs](https://docs.x.ai/overview)
- [dagster-anthropic integration](https://docs.dagster.io/integrations/libraries/anthropic) — the design `GrokResource` follows
- [Dagster University](https://courses.dagster.io/)
