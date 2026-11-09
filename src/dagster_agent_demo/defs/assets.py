"""
Agentic Orchestration Demo — data engineering use case
======================================================

Pipeline narrative
------------------
A feature request arrives (ticket / Jira / Slack / manual).
Dagster orchestrates a multi-step agentic workflow:

  1. ingest_requirements     — parse + normalise the raw request (LLM)
  2. analyze_and_plan        — agent decomposes into actionable tasks (LLM)
  3. generate_code           — Cursor cloud agent writes the code, opens a real PR

The PR is then reviewed by the repo's PR review gate (GitHub Actions +
Cursor SDK, see pr_review_gate/), which auto-approves or routes to a human.

Each step is a Dagster asset:
  * full lineage in the asset graph
  * metadata surfaced in the UI (token usage, plan, PR link)
  * retries, partitioning, and scheduling come for free
  * swap the model/provider by changing the GrokResource config — zero
    asset code changes required

Agentic orchestration capabilities addressed
--------------------------------------------
Capability                                Dagster primitive
----------------------------------------  -----------------------------------------
Loop / integrate native pipelines/gates   Asset dependencies; PR review gate in CI
End-to-end, cloud-ready                   Dagster+ agent / K8s executor
Long-running tasks                        run_timeout_seconds, async ops, sensors
Event-driven triggers                     Sensor picks up new requests from a queue
Reproducible code execution               code_version tags, config schema, run log
Produces artifacts (PRs, code, outputs)   MaterializeResult + metadata URLs
Multi-agent workflows                     Each asset = an agent step; compose freely
Multiple model providers                  Grok (spec/plan) + Cursor (code) resources
Off the shelf                             Custom GrokResource modelled on dagster-anthropic
"""

import json
import textwrap
from datetime import datetime, timezone

import dagster as dg
from dagster_agent_demo.resources import CursorResource, GrokResource


# ---------------------------------------------------------------------------
# Config schema — surfaced in the Dagster+ Launchpad UI before every run,
# giving operators explicit control over the request and model
# without any code changes. Swap the model field to change model per run.
# ---------------------------------------------------------------------------
class AgentPipelineConfig(dg.Config):
    feature_request: str = (
        "Write a single Dagster asset that accepts a pandas DataFrame of "
        "transactions (columns: transaction_id, amount, merchant_category) "
        "and returns a summary DataFrame with mean, std, and outlier_count "
        "per merchant_category, flagging amounts beyond 3 standard deviations."
    )
    model: str = "grok-4.7"                  # swap model per run via config
    max_tokens: int = 8192


def _call_grok(
    client,
    model: str,
    prompt: str,
    max_tokens: int = 8192,
) -> tuple[str, int]:
    """
    Thin wrapper around the xAI (OpenAI-compatible) chat completions API.
    Returns (text_content, total_tokens).

    Centralising the call here means swapping to a different model or
    adding streaming later is a one-line change — no asset code touched.
    """
    response = client.chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    text = (response.choices[0].message.content or "").strip()
    tokens = response.usage.total_tokens if response.usage else 0
    return text, tokens


def _extract_json(text: str) -> str:
    """
    Strip markdown code fences that LLMs sometimes wrap around JSON,
    then return the first JSON object or array found in the text.

    Handles responses like:
      ```json\\n{...}\\n```
      ```\\n{...}\\n```
      plain {...}
    """
    import re

    # Strip ```json ... ``` or ``` ... ``` fences
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fenced:
        text = fenced.group(1).strip()

    # Find the first { or [ and slice from there to the matching closer
    start = next((i for i, c in enumerate(text) if c in "{["), None)
    if start is not None:
        text = text[start:]

    return text.strip()


# ===========================================================================
# STEP 1 — Ingest & normalise the raw feature request
# ===========================================================================
@dg.asset(
    group_name="agentic_pipeline",
    kinds={"grok"},
    description=(
        "Receives a raw feature request (from config, Jira, Slack sensor, etc.) "
        "and uses an LLM to normalise it into a structured JSON spec. "
        "This is the pipeline's 'memory initialisation' step — the canonical "
        "representation that all downstream agents consume."
    ),
    code_version="v1",
    owners=["team:data-engineering"],
    tags={"layer": "ingestion", "agent_step": "1"},
)
def ingest_requirements(
    context: dg.AssetExecutionContext,
    config: AgentPipelineConfig,
    grok: GrokResource,
) -> dg.MaterializeResult:
    """
    LLM call: parse the raw feature request into a structured spec.

    In a real pipeline this could be triggered by:
      - a Sensor watching a Jira/ServiceNow queue
      - a schedule that polls a Slack channel
      - a manual Launchpad run with the request pasted into config
    """
    context.log.info("Ingesting feature request via LLM normalisation...")

    prompt = textwrap.dedent(f"""
        You are a senior data engineering analyst at a financial services firm.
        Parse the following feature request into a structured JSON specification
        with these fields:
          - title: short title
          - objective: one-sentence goal
          - acceptance_criteria: list of measurable outcomes
          - data_assets_affected: list of table/asset names (infer if not stated)
          - complexity: one of [low, medium, high]
          - estimated_hours: integer estimate

        Feature request:
        \"\"\"{config.feature_request}\"\"\"

        Respond ONLY with valid JSON. No markdown fences.
    """).strip()

    with grok.get_client(context) as client:
        raw_json, tokens = _call_grok(client, config.model, prompt, config.max_tokens)

    # Parse to validate — raises if LLM returned malformed JSON
    spec = json.loads(_extract_json(raw_json))

    context.log.info(f"Parsed spec: {json.dumps(spec, indent=2)}")

    return dg.MaterializeResult(
        metadata={
            # Stored as structured metadata — queryable in Dagster+ Insights
            "spec": dg.MetadataValue.json(spec),
            "model": dg.MetadataValue.text(config.model),
            "complexity": dg.MetadataValue.text(spec.get("complexity", "unknown")),
            "estimated_hours": dg.MetadataValue.int(spec.get("estimated_hours", 0)),
            "tokens_used": dg.MetadataValue.int(tokens),
            "timestamp": dg.MetadataValue.text(
                datetime.now(timezone.utc).isoformat()
            ),
        },
        value=spec,
    )


# ===========================================================================
# STEP 2 — Agent planning: decompose requirements into actionable tasks
# ===========================================================================
@dg.asset(
    group_name="agentic_pipeline",
    kinds={"grok"},
    description=(
        "Planning agent: reads the structured spec and decomposes it into an "
        "ordered task plan. Produces the 'scratchpad' that the code-generation "
        "agent will execute against. Demonstrates agent-to-agent hand-off via "
        "Dagster asset dependencies."
    ),
    code_version="v1",
    owners=["team:data-engineering"],
    tags={"layer": "planning", "agent_step": "2"},
)
def analyze_and_plan(
    context: dg.AssetExecutionContext,
    config: AgentPipelineConfig,
    grok: GrokResource,
    ingest_requirements: dict,  # upstream asset injected by name
) -> dg.MaterializeResult:
    """
    Planning agent — decomposes the spec into ordered implementation tasks,
    which become the Cursor agent's instructions in the next step.
    """
    context.log.info("Planning agent decomposing requirements into tasks...")

    spec_json = json.dumps(ingest_requirements, indent=2)

    prompt = textwrap.dedent(f"""
        You are a principal data engineer at a financial services firm.
        Given the following feature specification, produce a concise
        implementation plan scoped to a SINGLE Python file as JSON:
        {{
          "tasks": [
            {{
              "id": "T1",
              "title": "...",
              "description": "one sentence",
              "file_to_modify": "assets/transactions.py"
            }}
          ],
          "dependencies": {{"T2": ["T1"]}},
          "risks": ["..."],
          "reviewer_notes": "one sentence for the approving team"
        }}

        Rules:
        - Maximum 4 tasks total
        - All tasks target a single Python file
        - Keep descriptions to one sentence each
        - Focus only on the core Dagster asset implementation

        Feature specification:
        {spec_json}

        Respond ONLY with valid JSON. No markdown fences.
    """).strip()

    with grok.get_client(context) as client:
        raw_json, tokens = _call_grok(client, config.model, prompt, config.max_tokens)

    plan = json.loads(_extract_json(raw_json))

    task_count = len(plan.get("tasks", []))
    context.log.info(f"Plan produced: {task_count} tasks identified")

    return dg.MaterializeResult(
        metadata={
            "plan": dg.MetadataValue.json(plan),
            "task_count": dg.MetadataValue.int(task_count),
            "risks": dg.MetadataValue.json(plan.get("risks", [])),
            "reviewer_notes": dg.MetadataValue.text(
                plan.get("reviewer_notes", "")
            ),
            "tokens_used": dg.MetadataValue.int(tokens),
        },
        value=plan,
    )


# ===========================================================================
# STEP 3 — Code generation agent (Cursor cloud agent → real pull request)
# ===========================================================================
@dg.asset(
    group_name="agentic_pipeline",
    kinds={"cursor", "github"},
    description=(
        "Code-generation agent: a Cursor cloud agent implements the task plan "
        "in the real GitHub repo and opens a pull request, which the repo's PR "
        "review gate then reviews. Long-running step (typically a few minutes) "
        "— Dagster just waits on it like any other asset."
    ),
    code_version="v2",
    owners=["team:data-engineering"],
    tags={"layer": "generation", "agent_step": "3"},
)
def generate_code(
    context: dg.AssetExecutionContext,
    cursor: CursorResource,
    ingest_requirements: dict,
    analyze_and_plan: dict,
) -> dg.MaterializeResult:
    """
    Code-generation agent.

    Unlike the Grok steps, a Cursor agent works inside the repo: it can read
    existing code, run it, and open a PR. Quality gating happens on the PR,
    in the repo's GitHub Action (pr_review_gate/), not here.

    Multi-agent pattern: a different model/provider (Cursor) does the coding
    than the one that planned (Grok).
    """
    tasks = "\n".join(
        f"- {t.get('title', '')}: {t.get('description', '')}"
        for t in analyze_and_plan.get("tasks", [])
    )
    criteria = "\n".join(f"- {c}" for c in ingest_requirements.get("acceptance_criteria", []))

    prompt = textwrap.dedent(f"""
        Implement this feature request as a single Dagster asset.

        Title: {ingest_requirements.get("title", "Feature request")}
        Objective: {ingest_requirements.get("objective", "")}

        Tasks:
        {tasks}

        Acceptance criteria:
        {criteria}

        Follow the dagster-expert skill (.cursor/skills/dagster-expert) before
        writing any Dagster code: read its SKILL.md and the asset references
        it points to, and follow its conventions. Where the skill conflicts
        with the rules below (e.g. it suggests `dg scaffold` or a Component),
        the rules win.

        Rules:
        - Put the asset in ONE new file under generated/ (create the folder
          if needed). Do not modify any existing files.
        - Keep it short and readable: one @dg.asset, a one-line docstring.
        - Use only packages already in pyproject.toml (dagster and the
          standard library). Do not add dependencies.
        - Add a unit test next to it, generated/test_<asset_file>.py, that
          materializes the asset with dg.materialize([...]) and checks the result.

        Before opening the PR, verify the code works and fix anything that fails:
        - uv run python -m py_compile generated/*.py
        - uv run pytest generated/
        - uv run dg check defs   (the project must still load)
        In the PR description, list these commands and their results.

        Open a pull request titled "{ingest_requirements.get("title", "Feature request")}".
    """).strip()

    context.log.info("Starting Cursor cloud agent (this can take a few minutes)...")
    pr = cursor.open_pull_request(prompt, idempotency_key=f"{context.run_id}-generate_code")
    context.log.info(f"Cursor agent opened {pr['pr_url']}")

    return dg.MaterializeResult(
        metadata={
            "pr_url": dg.MetadataValue.url(pr["pr_url"]),
            "branch": dg.MetadataValue.text(pr["branch"]),
            "agent_summary": dg.MetadataValue.md(pr["summary"]),
            "cursor_agent_id": dg.MetadataValue.text(pr["agent_id"]),
            "duration_seconds": dg.MetadataValue.int(pr["duration_seconds"]),
        },
        value=pr,
    )
