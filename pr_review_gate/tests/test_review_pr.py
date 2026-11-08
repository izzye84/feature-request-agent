import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import review_pr
from review_pr import (
    COMMENT_MARKER,
    HUMAN_LABEL,
    ReviewRunError,
    build_model_selection,
    parse_model_params,
    publish,
    run_pr_review,
)

VERDICT = {
    "verdict": "auto_approve",
    "complexity": "trivial",
    "touches_auth": False,
    "secrets_findings": [],
    "auth_findings": [],
    "summary": "Fix a typo",
    "rationale": "docs only",
}

CATALOG = [
    SimpleNamespace(
        id="claude-sonnet-5-5",
        parameters=[
            SimpleNamespace(
                id="reasoning_effort",
                values=[SimpleNamespace(value=v) for v in ("low", "medium", "high")],
            )
        ],
    ),
    SimpleNamespace(id="composer-2.5", parameters=[]),
]


# --- model selection ---------------------------------------------------------
def test_parse_model_params_accepts_repeated_and_comma_separated():
    assert parse_model_params(["", "reasoning_effort=high,context=1m", "fast=true"]) == {
        "reasoning_effort": "high", "context": "1m", "fast": "true",
    }
    with pytest.raises(ValueError):
        parse_model_params(["reasoning_effort"])


def test_build_model_selection_matches_sdk_shape():
    selection = build_model_selection("claude-sonnet-5-5", {"reasoning_effort": "high"}, CATALOG)
    assert selection.id == "claude-sonnet-5-5"
    assert [(p.id, p.value) for p in selection.params] == [("reasoning_effort", "high")]


@pytest.mark.parametrize(
    "model_id,params",
    [
        ("auto-smart", {}),                                 # Router: not on this plan
        ("claude-sonnet-5-5", {"effort": "high"}),          # wrong param name for this model
        ("claude-sonnet-5-5", {"reasoning_effort": "max"}), # value not offered
    ],
)
def test_build_model_selection_rejects_what_the_catalog_does_not_offer(model_id, params):
    with pytest.raises(ValueError):
        build_model_selection(model_id, params, CATALOG)


# --- the SDK call ------------------------------------------------------------
def fake_create_agent(status="finished", text=json.dumps(VERDICT)):
    calls = []

    @contextmanager
    def create(**kwargs):
        calls.append(kwargs)
        result = SimpleNamespace(id="run-1", status=status, result=text, error_message=None)
        run = SimpleNamespace(id="run-1", wait=lambda: result)
        yield SimpleNamespace(
            agent_id="bc-1",
            send=lambda prompt: run,
            get_usage=lambda run_id=None: (_ for _ in ()).throw(RuntimeError("n/a")),
        )

    return create, calls


def review(**overrides):
    kwargs = dict(
        api_key="k", repo_url="https://github.com/a/b", pr_url="https://github.com/a/b/pull/1",
        model=build_model_selection("composer-2.5", {}), reviewer="octocat",
    )
    kwargs.update(overrides)
    return run_pr_review(**kwargs)


def test_run_pr_review_attaches_agent_to_pr_without_branch_access():
    create, calls = fake_create_agent()
    decision = review(create_agent=create, idempotency_key="123-1")
    cloud = calls[0]["cloud"]
    assert cloud.repos[0].pr_url == "https://github.com/a/b/pull/1"
    assert cloud.work_on_current_branch is False and cloud.auto_create_pr is False
    assert calls[0]["idempotency_key"] == "123-1"
    assert decision["route"] == "auto_approve" and decision["assignee"] is None


@pytest.mark.parametrize("status", ["error", "cancelled", "expired"])
def test_unfinished_run_raises(status):
    create, _ = fake_create_agent(status=status)
    with pytest.raises(ReviewRunError):
        review(create_agent=create)


def test_unparseable_verdict_keeps_raw_text():
    create, _ = fake_create_agent(text="I could not decide.")
    with pytest.raises(ReviewRunError) as exc:
        review(create_agent=create)
    assert exc.value.raw_text == "I could not decide."


# --- publishing --------------------------------------------------------------
class FakeGitHub:
    def __init__(self):
        self.actions = []

    def __getattr__(self, name):
        return lambda *args: self.actions.append((name, *args[1:]))


def decision(**overrides):
    create, _ = fake_create_agent(text=json.dumps({**VERDICT, **overrides}))
    return review(create_agent=create)


def test_auto_approve_publishes_comment_and_approval():
    gh = FakeGitHub()
    body = publish(gh, 7, decision())
    names = [a[0] for a in gh.actions]
    assert names == ["approve", "upsert_comment", "remove_label"]
    assert COMMENT_MARKER in body and "auto-approved" in body


def test_human_review_dismisses_stale_approval_labels_and_requests_reviewer():
    gh = FakeGitHub()
    publish(gh, 7, decision(secrets_findings=["config.py:3 — AWS key"]))
    names = [a[0] for a in gh.actions]
    assert names == ["upsert_comment", "dismiss_bot_approvals", "add_label", "request_reviewers"]
    assert ("add_label", HUMAN_LABEL) in gh.actions
    assert ("request_reviewers", ["octocat"]) in gh.actions


def test_comment_neutralises_links_mentions_and_html_from_agent_text():
    body = review_pr.render_comment(
        decision(summary="Ping @everyone, see https://evil.example <img src=x>")
    )
    assert "https://evil.example" not in body
    assert "@everyone" not in body
    assert "<img" not in body


def test_refused_approval_routes_to_human_and_never_claims_approved():
    import io
    import urllib.error

    gh = FakeGitHub()

    def refuse(*_):
        raise urllib.error.HTTPError(
            "u", 422, "Unprocessable", {},
            io.BytesIO(b'{"errors":["GitHub Actions is not permitted to approve pull requests."]}'),
        )

    gh.approve = refuse
    with pytest.raises(review_pr.ApprovalError) as exc:
        publish(gh, 7, decision())
    names = [a[0] for a in gh.actions]
    assert names == ["upsert_comment", "add_label"]
    assert "could not be submitted" in exc.value.body
    assert "### PR review gate: auto-approved" not in exc.value.body


# --- usage -------------------------------------------------------------------
def _usage(total, cost=None):
    tokens = SimpleNamespace(
        total_tokens=total, input_tokens=total, output_tokens=0, cache_read_tokens=0
    )
    return SimpleNamespace(usage=tokens, cost=cost)


def test_usage_snapshot_waits_for_billing_to_land():
    responses = iter([_usage(0), _usage(0), _usage(1200, SimpleNamespace(charged_cents=1.5))])
    agent = SimpleNamespace(get_usage=lambda run_id=None: next(responses))
    snap = review_pr._usage_snapshot(agent, "run-1", delay=0)
    assert snap["total_tokens"] == 1200 and snap["charged_cents"] == 1.5


def test_usage_snapshot_reports_pending_instead_of_zero():
    agent = SimpleNamespace(get_usage=lambda run_id=None: _usage(0))
    snap = review_pr._usage_snapshot(agent, "run-1", attempts=2, delay=0)
    assert snap["status"] == "pending" and "total_tokens" not in snap
