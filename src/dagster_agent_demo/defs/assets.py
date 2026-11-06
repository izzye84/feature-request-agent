"""
Agentic Orchestration Demo — data engineering use case
======================================================

Pipeline narrative
------------------
A feature request arrives (ticket / Jira / Slack / manual).
Dagster orchestrates a multi-step agentic workflow:

  1. ingest_requirements     — parse + normalise the raw request (LLM)
  2. analyze_and_plan        — agent decomposes into actionable tasks (LLM)
  3. generate_code           — agent produces implementation code (LLM)
  4. run_quality_gates       — automated guardrails check the output
  5. publish_artifact        — produce a durable PR-style artifact

Each step is a Dagster asset:
  * full lineage in the asset graph
  * metadata surfaced in the UI (token usage, scores, file paths)
  * retries, partitioning, and scheduling come for free
  * swap the model/provider by changing the AnthropicResource config — zero
    asset code changes required

Agentic orchestration capabilities addressed
--------------------------------------------
Capability                                Dagster primitive
----------------------------------------  -----------------------------------------
Loop / integrate native pipelines/gates   Asset dependencies + quality-gate asset
End-to-end, cloud-ready                   Dagster+ agent / K8s executor
Long-running tasks                        run_timeout_seconds, async ops, sensors
Feedback loops / memory                   Sensor triggers re-run; metadata as memory
Reproducible code execution               code_version tags, config schema, run log
Produces artifacts (PRs, code, outputs)   MaterializeResult + metadata URLs
Multi-agent workflows                     Each asset = an agent step; compose freely
Multiple model providers                  Swap AnthropicResource for any provider
Off the shelf                             dagster-anthropic ships today; copy-paste ready
"""

import json
import textwrap
from datetime import datetime, timezone

import dagster as dg
from dagster_anthropic import AnthropicResource


# ---------------------------------------------------------------------------
# Config schema — surfaced in the Dagster+ Launchpad UI before every run,
# giving operators explicit control over model, thresholds, and reviewer
# without any code changes. Swap the model field to change model per run.
# ---------------------------------------------------------------------------
class AgentPipelineConfig(dg.Config):
    feature_request: str = (
        "Write a single Dagster asset that accepts a pandas DataFrame of "
        "transactions (columns: transaction_id, amount, merchant_category) "
        "and returns a summary DataFrame with mean, std, and outlier_count "
        "per merchant_category, flagging amounts beyond 3 standard deviations."
    )
    model: str = "claude-sonnet-4-6"          # swap model per run via config
    max_tokens: int = 8192
    quality_gate_threshold: float = 0.35      # score below this blocks promotion
    reviewer: str = "data-eng-team"           # notional approval owner


def _call_claude(
    client,
    model: str,
    prompt: str,
    max_tokens: int = 8192,
) -> tuple[str, int]:
    """
    Thin wrapper around the Anthropic messages API.
    Returns (text_content, total_input_tokens + output_tokens).

    Centralising the call here means swapping to a different model or
    adding streaming later is a one-line change — no asset code touched.
    """
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()
    tokens = (response.usage.input_tokens or 0) + (response.usage.output_tokens or 0)
    return text, tokens


def _extract_json(text: str) -> str:
    """
    Strip markdown code fences that Claude sometimes wraps around JSON,
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
    compute_kind="claude",
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
    anthropic: AnthropicResource,
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

    with anthropic.get_client(context) as client:
        raw_json, tokens = _call_claude(client, config.model, prompt, config.max_tokens)

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
    compute_kind="claude",
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
    anthropic: AnthropicResource,
    ingest_requirements: dict,  # upstream asset injected by name
) -> dg.MaterializeResult:
    """
    Planning agent — decomposes the spec into ordered implementation tasks.

    This is the 'feedback loop' anchor: if quality gates fail downstream,
    a sensor re-materialises from this step with updated context, giving
    the agent a second pass with richer information.
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

    with anthropic.get_client(context) as client:
        raw_json, tokens = _call_claude(client, config.model, prompt, config.max_tokens)

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
# STEP 3 — Code generation agent
# ===========================================================================
@dg.asset(
    group_name="agentic_pipeline",
    compute_kind="claude",
    description=(
        "Code-generation agent: implements the task plan as production-ready "
        "Python/SQL. Outputs are stored as metadata artifacts — reproducible "
        "because the plan, model version, and config are all captured upstream."
    ),
    code_version="v1",
    owners=["team:data-engineering"],
    tags={"layer": "generation", "agent_step": "3"},
)
def generate_code(
    context: dg.AssetExecutionContext,
    config: AgentPipelineConfig,
    anthropic: AnthropicResource,
    ingest_requirements: dict,
    analyze_and_plan: dict,
) -> dg.MaterializeResult:
    """
    Code-generation agent.

    Takes the normalised spec + task plan and generates implementation code.
    The code is emitted as a metadata artifact — in a real pipeline you would
    write it to a branch in Git and open a PR via the GitHub API.

    Multi-agent pattern: this agent is completely unaware of how the plan was
    produced. You can hot-swap the planning agent without touching this asset.
    """
    context.log.info("Code-generation agent starting...")

    objective = ingest_requirements.get("objective", config.feature_request)

    prompt = textwrap.dedent(f"""
        Write a single Dagster @asset function in Python that implements
        the following objective. Be concise — maximum 50 lines total.

        Strict rules:
        - Start with: import dagster as dg / import pandas as pd
        - ONE @dg.asset function only, group_name="agentic_pipeline"
        - No helper functions, no classes, no tests
        - One-line docstring only
        - Must be complete and syntactically valid Python

        Objective: {objective}

        Respond ONLY with Python source code. No markdown fences.
    """).strip()

    with anthropic.get_client(context) as client:
        generated_code, tokens = _call_claude(
            client, config.model, prompt, max_tokens=1024
        )

    line_count = len(generated_code.splitlines())
    context.log.info(f"Generated {line_count} lines of code")

    return dg.MaterializeResult(
        metadata={
            "generated_code": dg.MetadataValue.text(generated_code),
            "line_count": dg.MetadataValue.int(line_count),
            "model": dg.MetadataValue.text(config.model),
            "tokens_used": dg.MetadataValue.int(tokens),
            # In production: replace with actual GitHub PR URL
            "artifact_url": dg.MetadataValue.url(
                "https://github.com/example-org/data-platform/pull/9999"
            ),
        },
        value={"code": generated_code, "line_count": line_count},
    )


# ===========================================================================
# STEP 4 — Quality gates (approval / guardrail check)
# ===========================================================================
@dg.asset(
    group_name="agentic_pipeline",
    compute_kind="claude",
    description=(
        "Automated quality gate: runs a set of guardrail checks against the "
        "generated code before it can be promoted. If the score is below the "
        "configured threshold the asset fails, blocking downstream promotion "
        "and triggering the feedback-loop sensor."
    ),
    code_version="v1",
    owners=["team:platform-engineering"],
    tags={"layer": "governance", "agent_step": "4"},
)
def run_quality_gates(
    context: dg.AssetExecutionContext,
    config: AgentPipelineConfig,
    anthropic: AnthropicResource,
    generate_code: dict,
    analyze_and_plan: dict,
) -> dg.MaterializeResult:
    """
    Quality gate asset — the 'approval / guardrail' step.

    Uses an LLM reviewer to score the generated code against the plan, then
    applies a hard threshold. Below threshold → asset fails → Dagster marks
    it as failed in the UI and can trigger alerts or re-runs.

    In a real pipeline you would add:
      - static analysis (ruff, mypy)
      - unit test execution
      - security scanning (Bandit, Semgrep)
      - human-approval sensor (wait for a Slack reaction / JIRA approval)
    """
    context.log.info("Running quality gates...")

    code = generate_code.get("code", "")
    objective = analyze_and_plan.get("reviewer_notes", "") or \
        analyze_and_plan.get("tasks", [{}])[0].get("description", "")

    review_prompt = textwrap.dedent(f"""
        You are a senior engineer reviewing a proof-of-concept Dagster asset.
        Score the code generously — this is a demo, not production.

        Evaluate ONLY these three criteria:
        1. Is the code syntactically complete (no truncated functions)?
        2. Does it implement the core data transformation described?
        3. Are there any hardcoded secrets or obvious security issues?

        Do NOT penalise for: missing tests, hardcoded sample data,
        missing database connections, incomplete error handling,
        naming differences, or missing imports that aren't used.

        Return JSON only:
        {{
          "overall_score": <float 0.0-1.0>,
          "completeness": <float 0.0-1.0>,
          "correctness": <float 0.0-1.0>,
          "security_concerns": [],
          "blocking_issues": [],
          "suggestions": ["..."],
          "approve": <true|false>
        }}

        Objective (for context): {objective}

        Code under review:
        {code[:3000]}

        Respond ONLY with valid JSON. No markdown fences.
    """).strip()

    with anthropic.get_client(context) as client:
        raw_json, tokens = _call_claude(
            client, config.model, review_prompt, config.max_tokens
        )

    review = json.loads(_extract_json(raw_json))
    score = float(review.get("overall_score", 0.0))
    approved = review.get("approve", False)

    context.log.info(
        f"Quality gate score: {score:.2f} (threshold: {config.quality_gate_threshold})"
    )

    gate_passed = score >= config.quality_gate_threshold and approved

    if not gate_passed:
        # Fail the asset — Dagster marks it red in the UI, blocks downstream
        # assets, and the feedback sensor can trigger a re-run with context
        raise dg.Failure(
            description=(
                f"Quality gate failed. Score {score:.2f} < threshold "
                f"{config.quality_gate_threshold}. "
                f"Blocking issues: {review.get('blocking_issues', [])}"
            ),
            metadata={
                "review": dg.MetadataValue.json(review),
                "score": dg.MetadataValue.float(score),
                "threshold": dg.MetadataValue.float(config.quality_gate_threshold),
            },
        )

    context.log.info("Quality gate PASSED — cleared for artifact publication")

    return dg.MaterializeResult(
        metadata={
            "review": dg.MetadataValue.json(review),
            "score": dg.MetadataValue.float(score),
            "approved": dg.MetadataValue.bool(approved),
            "security_concerns": dg.MetadataValue.json(
                review.get("security_concerns", [])
            ),
            "suggestions": dg.MetadataValue.json(review.get("suggestions", [])),
            "reviewer": dg.MetadataValue.text(config.reviewer),
            "tokens_used": dg.MetadataValue.int(tokens),
        },
        value=review,
    )


# ===========================================================================
# STEP 5 — Publish artifact (PR summary, generated files, lineage)
# ===========================================================================
@dg.asset(
    group_name="agentic_pipeline",
    description=(
        "Final step: assembles a complete PR-ready artifact package — "
        "the generated code, the review summary, the task plan, and full "
        "lineage metadata. In production this would open a GitHub PR and "
        "post a summary to Slack. Demonstrates Dagster's artifact-production "
        "and end-to-end lineage tracking."
    ),
    code_version="v1",
    owners=["team:data-engineering"],
    tags={"layer": "delivery", "agent_step": "5"},
)
def publish_artifact(
    context: dg.AssetExecutionContext,
    config: AgentPipelineConfig,
    ingest_requirements: dict,
    analyze_and_plan: dict,
    generate_code: dict,
    run_quality_gates: dict,
) -> dg.MaterializeResult:
    """
    Artifact publication step.

    Produces the durable output of the entire agentic workflow:
      - a self-contained PR description (markdown)
      - links to generated code
      - full quality gate summary
      - complete upstream lineage (Dagster tracks this automatically)

    In a real pipeline: call the GitHub API to open a PR, post to Slack,
    update the Jira ticket. All of those are Dagster resources / ops.
    """
    context.log.info("Assembling and publishing final artifact...")

    spec = ingest_requirements
    plan = analyze_and_plan
    code_artifact = generate_code
    review = run_quality_gates

    # Build a PR description (markdown) — in production, POST to GitHub API
    pr_body = textwrap.dedent(f"""
        ## {spec.get('title', 'Feature Implementation')}

        ### Objective
        {spec.get('objective', '')}

        ### Acceptance Criteria
        {chr(10).join(f"- {c}" for c in spec.get('acceptance_criteria', []))}

        ### Implementation Summary
        - **Tasks completed:** {len(plan.get('tasks', []))}
        - **Lines of code generated:** {code_artifact.get('line_count', 0)}
        - **Quality gate score:** {review.get('overall_score', 0):.0%}
        - **Security concerns:** {len(review.get('security_concerns', []))} flagged

        ### Reviewer Notes
        {plan.get('reviewer_notes', 'None')}

        ### Suggestions for Follow-up
        {chr(10).join(f"- {s}" for s in review.get('suggestions', []))}

        ---
        *Generated by Dagster agentic pipeline run `{context.run_id}`*
        *Model: `{config.model}` | Reviewer: `{config.reviewer}`*
    """).strip()

    context.log.info("Artifact assembled. In production: opening GitHub PR...")

    return dg.MaterializeResult(
        metadata={
            # The PR description rendered as markdown in the Dagster UI
            "pr_description": dg.MetadataValue.md(pr_body),
            # Notional PR URL — replace with real GitHub API call
            "pr_url": dg.MetadataValue.url(
                "https://github.com/example-org/data-platform/pull/9999"
            ),
            "run_id": dg.MetadataValue.text(context.run_id),
            "pipeline_complete": dg.MetadataValue.bool(True),
            "total_tasks": dg.MetadataValue.int(len(plan.get("tasks", []))),
            "quality_score": dg.MetadataValue.float(
                review.get("overall_score", 0.0)
            ),
        },
        value={"pr_body": pr_body, "status": "published"},
    )
