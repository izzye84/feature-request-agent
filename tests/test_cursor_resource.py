from contextlib import contextmanager
from types import SimpleNamespace

import dagster as dg
import pytest

from dagster_agent_demo.resources import CursorResource
from dagster_agent_demo.resources import cursor as cursor_module

PR = "https://github.com/org/repo/pull/7"


def fake_agent_create(monkeypatch, *, status="finished", pr_url=PR):
    calls = []
    git = SimpleNamespace(branches=[SimpleNamespace(branch="cursor/feature", pr_url=pr_url)])
    result = SimpleNamespace(
        status=status, git=git, result="Added generated/outliers.py",
        agent_id="bc-1", duration_ms=95_000, error_message=None,
    )

    @contextmanager
    def create(**kwargs):
        calls.append(kwargs)
        yield SimpleNamespace(send=lambda prompt: SimpleNamespace(wait=lambda: result))

    monkeypatch.setattr(cursor_module.Agent, "create", create)
    return calls


def resource():
    return CursorResource(api_key="k", repo_url="https://github.com/org/repo")


def test_open_pull_request_returns_the_pr(monkeypatch):
    calls = fake_agent_create(monkeypatch)
    pr = resource().open_pull_request("do it", idempotency_key="run-1")
    assert pr == {
        "pr_url": PR, "branch": "cursor/feature", "summary": "Added generated/outliers.py",
        "agent_id": "bc-1", "duration_seconds": 95,
    }
    cloud = calls[0]["cloud"]
    assert cloud.auto_create_pr is True
    assert cloud.repos[0].starting_ref == "main"
    assert calls[0]["idempotency_key"] == "run-1"


def test_failed_run_fails_the_asset(monkeypatch):
    fake_agent_create(monkeypatch, status="error")
    with pytest.raises(dg.Failure):
        resource().open_pull_request("do it")


def test_run_without_a_pr_fails_the_asset(monkeypatch):
    fake_agent_create(monkeypatch, pr_url="")
    with pytest.raises(dg.Failure, match="without opening a pull request"):
        resource().open_pull_request("do it")
