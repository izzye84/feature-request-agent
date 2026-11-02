"""
Sensors — feedback loops and automation triggers
=================================================

Sensors are Dagster's event-driven automation primitive. They run on a tick
(default every 30 seconds) and can:
  - watch external systems (Jira queues, S3 buckets, Slack channels)
  - react to asset materialisation events
  - react to run success / failure events
  - submit new runs with updated configuration

This file contains two sensors:

1. quality_gate_failure_sensor
   Watches for failed `run_quality_gates` assets and automatically re-runs
   the pipeline with a lower threshold — the 'agent reflects and retries' pattern.

2. new_feature_request_sensor
   Watches a local JSON file (requests/queue.json) for new feature requests
   and triggers the pipeline automatically. The file acts as a stand-in for
   any real queue (Jira, ServiceNow, S3) — swap the file-read for an API call
   to connect to a real system. The sensor cursor prevents duplicate processing.

   To demo: drop a new entry into requests/queue.json while the sensor is
   running and watch Dagster pick it up on the next tick.
"""

import json
from pathlib import Path

import dagster as dg


# Path to the local queue file — relative to the project root
_QUEUE_FILE = Path(__file__).parents[4] / "requests" / "queue.json"


# ---------------------------------------------------------------------------
# 1. Feedback-loop sensor: re-run on quality-gate failure
# ---------------------------------------------------------------------------
@dg.run_failure_sensor(
    monitored_jobs=[],          # monitors ALL jobs by default in Dagster+
    description=(
        "Feedback loop: when a run fails at the quality_gate step, this sensor "
        "automatically re-submits with a lower threshold and annotated context. "
        "This is the 'agent reflects and retries' pattern."
    ),
    default_status=dg.DefaultSensorStatus.STOPPED,  # enable manually in UI for demo
)
def quality_gate_failure_sensor(
    context: dg.RunFailureSensorContext,
) -> dg.RunRequest | dg.SkipReason:
    """
    Triggered whenever any pipeline run fails.

    If the failure was a quality-gate failure, re-submit with a relaxed
    threshold — demonstrating the feedback loop / auto-remediation pattern.
    """
    failure_msg = str(context.failure_event.message or "")

    if "Quality gate failed" not in failure_msg:
        return dg.SkipReason("Failure was not a quality gate failure — skipping.")

    context.log.info(
        f"Quality gate failure detected in run {context.dagster_run.run_id}. "
        "Re-submitting with relaxed threshold..."
    )

    original_config = context.dagster_run.run_config or {}
    original_ops_config = original_config.get("ops", {})

    prev_threshold = 0.8
    try:
        prev_threshold = (
            original_ops_config
            .get("run_quality_gates", {})
            .get("config", {})
            .get("quality_gate_threshold", 0.8)
        )
    except (KeyError, TypeError):
        pass

    new_threshold = max(0.1, round(prev_threshold - 0.1, 2))

    context.log.info(
        f"Lowering quality_gate_threshold: {prev_threshold} → {new_threshold}"
    )

    return dg.RunRequest(
        run_key=f"retry-qg-{context.dagster_run.run_id[:8]}",
        run_config={
            "resources": original_config.get("resources", {}),
            "ops": {
                **original_ops_config,
                "run_quality_gates": {
                    "config": {
                        "quality_gate_threshold": new_threshold,
                    }
                },
            },
        },
        tags={
            "retry_of": context.dagster_run.run_id,
            "trigger": "quality_gate_failure_sensor",
        },
    )


# ---------------------------------------------------------------------------
# 2. New-request sensor: watch a local queue file
# ---------------------------------------------------------------------------
@dg.sensor(
    description=(
        "Watches requests/queue.json for new feature requests and triggers "
        "the agentic pipeline automatically. Each entry is processed once "
        "(tracked via cursor). In production, replace the file read with a "
        "Jira/ServiceNow/S3 API call — the sensor logic stays identical."
    ),
    minimum_interval_seconds=30,
    default_status=dg.DefaultSensorStatus.STOPPED,  # enable manually for demo
)
def new_feature_request_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.RunRequest | dg.SkipReason:
    """
    Polls requests/queue.json for new feature requests.

    Queue file format (requests/queue.json):
    [
      {
        "id": "FR-001",
        "feature_request": "Plain English description of the feature...",
        "model": "claude-sonnet-4-6",          (optional, uses default if omitted)
        "quality_gate_threshold": 0.35         (optional, uses default if omitted)
      }
    ]

    To demo without a real integration:
      1. Enable this sensor in the Dagster UI (Automation tab)
      2. Add a new entry to requests/queue.json (copy the example above)
      3. Wait up to 30 seconds for the next sensor tick
      4. Watch the run appear in the Runs tab

    The sensor cursor persists processed IDs between ticks, so adding the
    same ID twice will not trigger a duplicate run.

    To connect to a real queue, replace the file read below with:
      - requests.get(JIRA_API_URL, headers=auth_headers).json()
      - boto3.client('s3').get_object(Bucket=..., Key=...) for S3
      - Any other API call that returns a list of request dicts
    """
    # Restore cursor (persistent memory across ticks)
    cursor_data = json.loads(context.cursor or "{}")
    already_processed: list[str] = cursor_data.get("processed_ids", [])

    # Read the local queue file — swap this block for any external API call
    if not _QUEUE_FILE.exists():
        return dg.SkipReason(
            f"Queue file not found at {_QUEUE_FILE}. "
            "Create it to start triggering runs (see sensor docstring for format)."
        )

    try:
        requests_in_queue: list[dict] = json.loads(_QUEUE_FILE.read_text())
    except (json.JSONDecodeError, OSError) as e:
        return dg.SkipReason(f"Could not read queue file: {e}")

    new_requests = [
        r for r in requests_in_queue
        if r.get("id") and r["id"] not in already_processed
    ]

    if not new_requests:
        return dg.SkipReason(
            f"No new requests in queue ({len(requests_in_queue)} already processed)."
        )

    run_requests = []
    newly_processed = list(already_processed)

    for req in new_requests:
        context.log.info(f"New feature request detected: {req['id']}")
        run_requests.append(
            dg.RunRequest(
                run_key=req["id"],
                run_config={
                    "ops": {
                        "ingest_requirements": {
                            "config": {
                                "feature_request": req["feature_request"],
                                "model": req.get("model", "claude-sonnet-4-6"),
                                "quality_gate_threshold": req.get(
                                    "quality_gate_threshold", 0.35
                                ),
                                "reviewer": req.get("reviewer", "data-eng-team"),
                            }
                        }
                    }
                },
                tags={
                    "source": "new_feature_request_sensor",
                    "request_id": req["id"],
                },
            )
        )
        newly_processed.append(req["id"])

    # Persist updated cursor — Dagster stores this between ticks
    context.update_cursor(json.dumps({"processed_ids": newly_processed}))

    return run_requests
