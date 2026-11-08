"""PR review gate — a Cursor cloud agent reviews every PR; Python decides the route.

Runs from .github/workflows/pr-review.yml on each pull request, or locally:

    uv run review_pr.py --list-models
    uv run review_pr.py \\
      --repo-url https://github.com/izzye84/feature-request-agent \\
      --pr-url https://github.com/izzye84/feature-request-agent/pull/1 \\
      --model-id claude-sonnet-5-5 --model-param reasoning_effort=high

Trigger → output:
  1. GitHub `pull_request` event runs this script (code checked out from the base
     branch, so a PR cannot rewrite its own review rules).
  2. build_model_selection(): model id + params, validated against the catalog.
  3. run_pr_review(): the only Cursor SDK call — a cloud agent attached to the
     PR returns a JSON verdict.
  4. apply_policy() (review_policy.py): deterministic auto-approve vs human.
  5. publish(): this code, not the agent, comments / approves / labels with
     GITHUB_TOKEN. The agent never holds a GitHub write token.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from cursor_sdk import (
    Agent,
    CloudAgentOptions,
    CloudRepository,
    Cursor,
    CursorAgentError,
    ModelParameterValue,
    ModelSelection,
)

from review_policy import REVIEW_PROMPT, apply_policy, extract_json

DEFAULT_MODEL_ID = "composer-2.5"
COMMENT_MARKER = "<!-- cursor-pr-review-gate -->"
BOT_LOGIN = "github-actions[bot]"
HUMAN_LABEL = "needs-human-review"


class ReviewRunError(RuntimeError):
    """The cloud run did not finish, or its verdict could not be parsed."""

    def __init__(self, message: str, raw_text: str = "") -> None:
        super().__init__(message)
        self.raw_text = raw_text


# --- model selection ---------------------------------------------------------
def parse_model_params(pairs: Iterable[str]) -> dict[str, str]:
    """Parse ``key=value`` pairs (repeatable and/or comma-separated)."""
    params: dict[str, str] = {}
    for pair in pairs:
        for item in pair.split(","):
            item = item.strip()
            if not item:
                continue
            key, sep, value = item.partition("=")
            if not sep or not key.strip() or not value.strip():
                raise ValueError(f"model param must be key=value, got {item!r}")
            params[key.strip()] = value.strip()
    return params


def build_model_selection(
    model_id: str, params: dict[str, str], catalog: Sequence[Any] | None = None
) -> ModelSelection:
    """Build the SDK's ModelSelection, validating against the account catalog.

    Parameter names differ per model (``fast``, ``effort``, ``reasoning``,
    ``reasoning_effort``...), so a typo is caught here instead of mid-run.
    """
    if catalog is not None:
        model = next((m for m in catalog if m.id == model_id), None)
        if model is None:
            raise ValueError(
                f"model {model_id!r} is not available to this Cursor account "
                "(see `review_pr.py --list-models`)"
            )
        allowed = {p.id: {v.value for v in p.values} for p in (model.parameters or [])}
        for key, value in params.items():
            if key not in allowed:
                raise ValueError(
                    f"{model_id} has no parameter {key!r}; valid: {sorted(allowed) or 'none'}"
                )
            if value not in allowed[key]:
                raise ValueError(f"{model_id} {key} must be one of {sorted(allowed[key])}")
    return ModelSelection(
        id=model_id,
        params=[ModelParameterValue(id=k, value=v) for k, v in params.items()],
    )


def _describe_model(model: ModelSelection) -> str:
    params = ", ".join(f"{p.id}={p.value}" for p in model.params)
    return f"{model.id} ({params})" if params else model.id


# --- the Cursor SDK call -----------------------------------------------------
def _usage_snapshot(
    agent: Agent, run_id: str | None, *, attempts: int = 6, delay: float = 5.0
) -> dict[str, Any]:
    """Billed usage for the run. Cursor's usage/cost is eventually consistent:
    right after a run ends the backend often returns nothing, which the SDK
    decodes as all-zero counts. Poll briefly, and report "pending" rather than
    a misleading 0 if it still hasn't landed."""
    usage = None
    for attempt in range(attempts):
        try:
            usage = agent.get_usage(run_id=run_id) if run_id else agent.get_usage()
        except Exception as exc:  # billing snapshot is best-effort
            return {"error": str(exc)}
        if usage.usage.total_tokens > 0:
            break
        if attempt < attempts - 1:
            time.sleep(delay)
    else:
        return {"status": "pending", "note": "usage not reported yet; see the Cursor dashboard"}

    cost = usage.cost
    return {
        "total_tokens": usage.usage.total_tokens,
        "input_tokens": usage.usage.input_tokens,
        "output_tokens": usage.usage.output_tokens,
        "cache_read_tokens": usage.usage.cache_read_tokens,
        # 0 is legitimate for plan-included usage; None means cost not reported yet.
        "charged_cents": None if cost is None else cost.charged_cents,
    }


def run_pr_review(
    *,
    api_key: str,
    repo_url: str,
    pr_url: str,
    model: ModelSelection,
    reviewer: str | None = None,
    idempotency_key: str | None = None,
    create_agent: Callable[..., Any] = Agent.create,
) -> dict[str, Any]:
    """Create a cloud agent on the PR, wait for its verdict, then apply the policy."""
    with create_agent(
        model=model,
        api_key=api_key,
        name=f"PR review gate ({pr_url})",
        idempotency_key=idempotency_key,
        cloud=CloudAgentOptions(
            repos=[CloudRepository(url=repo_url, pr_url=pr_url)],
            auto_create_pr=False,
            skip_reviewer_request=True,
            # Tool restrictions are local-only, so a cloud agent can still
            # edit files. Never let those edits land on the PR under review.
            work_on_current_branch=False,
        ),
    ) as agent:
        run = agent.send(REVIEW_PROMPT)
        print(f"agent_id={agent.agent_id} run_id={run.id}", file=sys.stderr)
        result = run.wait()

        raw_text = result.result or ""
        if result.status != "finished":
            raise ReviewRunError(
                f"cloud review run {result.id} ended with status {result.status!r}"
                + (f": {result.error_message}" if result.error_message else ""),
                raw_text,
            )
        try:
            verdict = extract_json(raw_text)
        except ValueError as err:  # includes json.JSONDecodeError
            raise ReviewRunError(f"could not parse agent verdict: {err}", raw_text) from err

        decision = apply_policy(verdict)
        decision.update(
            {
                "agent_id": agent.agent_id,
                "run_id": result.id,
                "model": _describe_model(model),
                "repo_url": repo_url,
                "pr_url": pr_url,
                "assignee": None if decision["route"] == "auto_approve" else reviewer,
                "usage": _usage_snapshot(agent, result.id),
            }
        )
        return decision


# --- publishing to GitHub (deterministic code, never the agent) --------------
class GitHub:
    """Minimal stdlib GitHub REST client scoped to one repository."""

    def __init__(self, token: str, repo: str, api: str = "https://api.github.com") -> None:
        self.token = token
        self.repo = repo
        self.api = api

    def _request(self, method: str, path: str, body: Any = None) -> Any:
        req = urllib.request.Request(
            f"{self.api}/repos/{self.repo}{path}",
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
        return json.loads(data) if data else None

    def upsert_comment(self, pr_number: int, body: str) -> None:
        """Edit the gate's own comment if it exists, so pushes don't stack comments."""
        comments = self._request("GET", f"/issues/{pr_number}/comments?per_page=100")
        mine = next(
            (c for c in comments if c["user"]["login"] == BOT_LOGIN and COMMENT_MARKER in c["body"]),
            None,
        )
        if mine:
            self._request("PATCH", f"/issues/comments/{mine['id']}", {"body": body})
        else:
            self._request("POST", f"/issues/{pr_number}/comments", {"body": body})

    def approve(self, pr_number: int, body: str) -> None:
        self._request("POST", f"/pulls/{pr_number}/reviews", {"event": "APPROVE", "body": body})

    def dismiss_bot_approvals(self, pr_number: int, message: str) -> None:
        """A later push that fails the bar must not ride on an earlier approval."""
        for review in self._request("GET", f"/pulls/{pr_number}/reviews?per_page=100"):
            if review["user"]["login"] == BOT_LOGIN and review["state"] == "APPROVED":
                self._request(
                    "PUT",
                    f"/pulls/{pr_number}/reviews/{review['id']}/dismissals",
                    {"message": message, "event": "DISMISS"},
                )

    def add_label(self, pr_number: int, label: str) -> None:
        self._request("POST", f"/issues/{pr_number}/labels", {"labels": [label]})

    def remove_label(self, pr_number: int, label: str) -> None:
        try:
            self._request("DELETE", f"/issues/{pr_number}/labels/{urllib.parse.quote(label)}")
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise

    def request_reviewers(self, pr_number: int, reviewers: list[str]) -> None:
        self._request("POST", f"/pulls/{pr_number}/requested_reviewers", {"reviewers": reviewers})


def _sanitize(text: Any, limit: int = 1500) -> str:
    """Agent text is derived from untrusted PR content: no HTML, links, or pings."""
    text = re.sub(r"<[^>]*>", "", str(text or ""))
    text = re.sub(r"https?://\S+", "[link removed]", text)
    text = text.replace("@", "@​")
    return text[:limit]


def render_comment(decision: dict[str, Any], approval_error: str | None = None) -> str:
    approved = decision["route"] == "auto_approve"
    if approval_error:
        headline = "auto-approve recommended, but the approval could not be submitted"
    else:
        headline = "auto-approved" if approved else "human review required"
    findings = [*decision["secrets_findings"], *decision["auth_findings"]]
    lines = [
        COMMENT_MARKER,
        f"### PR review gate: {headline}",
        "",
    ]
    if approval_error:
        lines += [
            f"> [!WARNING]\n> GitHub rejected the approval: {_sanitize(approval_error, 300)}  ",
            "> A human must review this PR until the gate can approve.",
            "",
        ]
    lines += [
        f"**Blockers:** {', '.join(decision['blockers']) or 'none'}  ",
        f"**Complexity:** {decision['complexity']} · "
        f"**Touches auth:** {'yes' if decision['touches_auth'] else 'no'} · "
        f"**Model:** `{decision['model']}`",
        "",
        f"**Summary:** {_sanitize(decision['summary'])}",
        "",
        f"**Rationale:** {_sanitize(decision['rationale'])}",
    ]
    if findings:
        lines += ["", "<details><summary>Findings</summary>", ""]
        lines += [f"- {_sanitize(f, 300)}" for f in findings[:20]]
        lines += ["", "</details>"]
    lines += [
        "",
        f"<sub>Cursor cloud agent `{decision['agent_id']}` · run `{decision['run_id']}` · "
        f"tokens {decision['usage'].get('total_tokens', 'n/a')}. "
        "The model recommends; `apply_policy()` decides.</sub>",
    ]
    return "\n".join(lines)


def _attempt(description: str, action: Callable[[], None]) -> None:
    """Side effects after the comment are best-effort; surface failures as warnings."""
    try:
        action()
    except urllib.error.HTTPError as err:
        detail = err.read().decode(errors="replace")[:300]
        print(f"::warning::{description} failed ({err.code}): {detail}", file=sys.stderr)


class ApprovalError(Exception):
    """GitHub refused the approval; the PR was routed to a human instead."""

    def __init__(self, message: str, body: str) -> None:
        super().__init__(message)
        self.body = body


def publish(gh: GitHub, pr_number: int, decision: dict[str, Any]) -> str:
    """Comment / approve / label. Raises ApprovalError (after publishing) if approval is refused."""
    if decision["route"] == "auto_approve":
        # Approve *before* commenting so the comment never claims an approval
        # that didn't happen. Needs "Allow GitHub Actions to create and approve
        # pull requests" (Settings -> Actions -> General).
        try:
            gh.approve(pr_number, "Auto-approved by the PR review gate (see summary comment).")
        except urllib.error.HTTPError as err:
            detail = f"{err.code}: {err.read().decode(errors='replace')[:300]}"
            body = render_comment(decision, approval_error=detail)
            gh.upsert_comment(pr_number, body)
            _attempt("add label", lambda: gh.add_label(pr_number, HUMAN_LABEL))
            raise ApprovalError(f"approve failed ({detail})", body) from err
        body = render_comment(decision)
        gh.upsert_comment(pr_number, body)
        _attempt("remove label", lambda: gh.remove_label(pr_number, HUMAN_LABEL))
        return body

    body = render_comment(decision)
    gh.upsert_comment(pr_number, body)
    _attempt(
        "dismiss stale approval",
        lambda: gh.dismiss_bot_approvals(
            pr_number, "New changes no longer meet the auto-approve bar."
        ),
    )
    _attempt("add label", lambda: gh.add_label(pr_number, HUMAN_LABEL))
    if decision["assignee"]:
        _attempt(
            "request reviewer",
            lambda: gh.request_reviewers(pr_number, [decision["assignee"]]),
        )
    return body


def publish_failure(gh: GitHub, pr_number: int, error: str) -> None:
    """Fail safe: if the gate can't decide, a human reviews."""
    gh.upsert_comment(
        pr_number,
        f"{COMMENT_MARKER}\n### PR review gate: human review required\n\n"
        f"The automated review could not complete ({_sanitize(error, 300)}), "
        "so this PR was routed to a human.",
    )
    _attempt(
        "dismiss stale approval",
        lambda: gh.dismiss_bot_approvals(pr_number, "Automated review could not complete."),
    )
    _attempt("add label", lambda: gh.add_label(pr_number, HUMAN_LABEL))


def _write_step_summary(markdown: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(markdown + "\n")


# --- CLI ---------------------------------------------------------------------
def _list_models(api_key: str) -> int:
    for model in Cursor.models.list(api_key=api_key):
        params = "; ".join(
            f"{p.id}={'|'.join(v.value for v in p.values)}" for p in (model.parameters or [])
        )
        print(f"{model.id:<28} {params}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Review a PR with a Cursor cloud agent and route auto-approve vs human."
    )
    parser.add_argument("--repo-url", help="Repository the PR belongs to.")
    parser.add_argument("--pr-url", help="Pull request to attach the agent to.")
    parser.add_argument(
        "--model-id",
        default=os.environ.get("CURSOR_MODEL_ID") or DEFAULT_MODEL_ID,
        help="Model id from --list-models (env: CURSOR_MODEL_ID).",
    )
    parser.add_argument(
        "--model-param",
        action="append",
        default=[os.environ.get("CURSOR_MODEL_PARAMS", "")],
        metavar="KEY=VALUE",
        help="Per-model parameter, repeatable (env: CURSOR_MODEL_PARAMS, comma-separated).",
    )
    parser.add_argument(
        "--reviewer",
        default=os.environ.get("PR_REVIEWER") or None,
        help="GitHub login to request when a human must review (env: PR_REVIEWER).",
    )
    parser.add_argument("--idempotency-key", help="Reuse one cloud agent across retries.")
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Comment/approve/label on the PR (needs GITHUB_TOKEN, GITHUB_REPOSITORY, --pr-number).",
    )
    parser.add_argument("--pr-number", type=int)
    parser.add_argument("--list-models", action="store_true", help="Print the model catalog.")
    args = parser.parse_args(argv)

    api_key = os.environ.get("CURSOR_API_KEY", "").strip()
    if not api_key:
        print("CURSOR_API_KEY is not set", file=sys.stderr)
        return 1
    if args.list_models:
        return _list_models(api_key)
    if not (args.repo_url and args.pr_url):
        parser.error("--repo-url and --pr-url are required")

    gh = None
    if args.publish:
        token, repo = os.environ.get("GITHUB_TOKEN"), os.environ.get("GITHUB_REPOSITORY")
        if not (token and repo and args.pr_number):
            parser.error("--publish needs GITHUB_TOKEN, GITHUB_REPOSITORY and --pr-number")
        gh = GitHub(token, repo)

    try:
        params = parse_model_params(args.model_param)
        try:
            catalog = Cursor.models.list(api_key=api_key)
        except CursorAgentError as err:
            print(f"::warning::could not load model catalog, skipping validation: {err}", file=sys.stderr)
            catalog = None
        model = build_model_selection(args.model_id, params, catalog)
    except (ValueError, CursorAgentError) as err:
        print(f"invalid model selection: {err}", file=sys.stderr)
        return 1

    try:
        decision = run_pr_review(
            api_key=api_key,
            repo_url=args.repo_url,
            pr_url=args.pr_url,
            model=model,
            reviewer=args.reviewer,
            idempotency_key=args.idempotency_key,
        )
    except (CursorAgentError, ReviewRunError) as err:
        print(f"review failed: {err}", file=sys.stderr)
        if isinstance(err, ReviewRunError) and err.raw_text:
            print(f"--- agent output ---\n{err.raw_text}", file=sys.stderr)
        if gh:
            publish_failure(gh, args.pr_number, str(err))
        return 2

    json.dump(decision, sys.stdout, indent=2)
    sys.stdout.write("\n")
    if gh:
        try:
            _write_step_summary(publish(gh, args.pr_number, decision))
        except ApprovalError as err:
            _write_step_summary(err.body)
            print(f"::error::{err}", file=sys.stderr)
            return 4
        return 0
    return 0 if decision["route"] == "auto_approve" else 3


if __name__ == "__main__":
    raise SystemExit(main())
