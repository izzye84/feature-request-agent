"""
Sensors — automation triggers
==============================

Sensors are Dagster's event-driven automation primitive. They run on a tick
(default every 30 seconds) and can:
  - watch external systems (Jira queues, S3 buckets, Slack channels)
  - react to asset materialisation events
  - react to run success / failure events
  - submit new runs with updated configuration

new_feature_request_sensor
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
_QUEUE_FILE = Path(__file__).resolve().parents[3] / "requests" / "queue.json"


# ---------------------------------------------------------------------------
# New-request sensor: watch a local queue file
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
        "model": "grok-4.7"           (optional, uses default if omitted)
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
                                "model": req.get("model", "grok-4.7"),
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
