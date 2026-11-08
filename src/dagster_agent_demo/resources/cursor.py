"""
CursorResource — run a Cursor cloud agent against a GitHub repo
===============================================================

A Cursor cloud agent clones the repo on Cursor's machines, writes the code,
and opens a pull request. From Dagster's point of view it's one blocking
call: send a prompt, wait, get back the PR. Reviewing the PR is left to the
repo's PR review gate (GitHub Actions).

Requires Cursor's GitHub App to have access to the repo
(https://cursor.com/dashboard/integrations).
"""

from cursor_sdk import Agent, CloudAgentOptions, CloudRepository
from pydantic import Field

import dagster as dg


class CursorResource(dg.ConfigurableResource):
    """Opens pull requests by running a Cursor cloud agent on ``repo_url``.

    Examples:
        .. code-block:: python

            @dg.asset(kinds={"cursor"})
            def feature_pr(cursor: CursorResource):
                pr = cursor.open_pull_request("Add a README badge for CI status")
                return dg.MaterializeResult(metadata={"pr_url": dg.MetadataValue.url(pr["pr_url"])})
    """

    api_key: str = Field(description="Cursor API key (cursor.com/dashboard → API keys).")
    repo_url: str = Field(description="GitHub repo the agent works in, e.g. https://github.com/org/repo")
    base_branch: str = Field(default="main", description="Branch the agent starts from.")
    model: str = Field(default="composer-2.5", description="Cursor model id.")

    def open_pull_request(self, prompt: str, idempotency_key: str | None = None) -> dict:
        """Run a cloud agent with ``prompt`` and return the PR it opened.

        ``idempotency_key`` makes a retried Dagster step reattach to the same
        agent instead of starting (and paying for) a second one.
        """
        with Agent.create(
            model=self.model,
            api_key=self.api_key,
            idempotency_key=idempotency_key,
            cloud=CloudAgentOptions(
                repos=[CloudRepository(url=self.repo_url, starting_ref=self.base_branch)],
                auto_create_pr=True,
            ),
        ) as agent:
            result = agent.send(prompt).wait()

        if result.status != "finished":
            raise dg.Failure(f"Cursor agent run ended with status {result.status!r}: {result.error_message}")

        branches = result.git.branches if result.git else ()
        if not branches or not branches[0].pr_url:
            raise dg.Failure("Cursor agent finished without opening a pull request.")

        return {
            "pr_url": branches[0].pr_url,
            "branch": branches[0].branch,
            "summary": result.result,
            "agent_id": result.agent_id,
            "duration_seconds": round(result.duration_ms / 1000),
        }
