# PR review gate (Cursor SDK + GitHub Actions)

Every pull request opened against this repo is reviewed automatically. A Cursor cloud agent reads the PR and recommends a verdict; a deterministic Python policy decides whether to auto-approve or route it to a human. The gate runs entirely in GitHub Actions. It's a standalone uv project with one dependency (`cursor-sdk`) and doesn't use the Dagster project in this repo.

```
pull_request (opened / synchronize / reopened / ready_for_review)
  → .github/workflows/pr-review.yml       checks out the BASE commit, runs review_pr.py
  → review_pr.py  build_model_selection()  model id + params, validated against Cursor's catalog
  → review_pr.py  run_pr_review()          ← Cursor SDK: cloud agent attached to the PR, JSON verdict
  → review_policy.py  apply_policy()       secrets / auth / complexity / model recommendation
  → review_pr.py  publish()                comment + approve, or dismiss stale approval + label + reviewer
```

| Design decision | Why |
|---|---|
| The model recommends, `apply_policy()` decides | Auto-approval is an auditable, unit-tested rule. The model can only tighten the gate (`human_review` is always honored), never loosen it |
| The agent never gets a GitHub write token | All side effects are plain Python using the job's scoped `GITHUB_TOKEN` |
| Checkout of the base commit | A PR can't edit the policy that judges it |
| `work_on_current_branch=False` | Cursor's tool restrictions are local-only; any stray agent edit lands on a throwaway branch, never the PR |
| Agent text is sanitized before posting | Links, HTML and @mentions from PR-derived text are neutralized |
| Fail safe | Any agent/parse failure posts a "routed to a human" comment and labels the PR |

## Setup (once)

1. Merge this workflow and `pr_review_gate/` to `main` first: the job runs the base branch's code, so the PR that introduces it can't review itself.
2. Repo **Settings → Secrets and variables → Actions**:
   - Secret `CURSOR_API_KEY` (from [Cursor Dashboard → Integrations](https://cursor.com/dashboard/integrations)).
   - Optional variables: `CURSOR_MODEL_ID` (default `composer-2.5`), `CURSOR_MODEL_PARAMS` (e.g. `reasoning_effort=high`), `PR_REVIEWER` (a collaborator's GitHub login).
3. **Settings → Actions → General →** enable *Allow GitHub Actions to create and approve pull requests* so the bot can approve.
4. Connect the repo to Cursor's GitHub integration so the cloud agent can clone it.

## Swapping the model

The model is the SDK's `ModelSelection(id, params)`. Parameter names differ per model (`fast`, `effort`, `reasoning`, `reasoning_effort`...), so `review_pr.py` validates them against your account's catalog before starting a paid run:

```bash
cd pr_review_gate
uv run --env-file ../.env python review_pr.py --list-models
```

In CI, change the `CURSOR_MODEL_ID` / `CURSOR_MODEL_PARAMS` repo variables, with no code change. (Cursor Router presets — `auto-smart` with `optimize_for` — exist only on Teams/Enterprise plans.)

## Running it locally

```bash
uv run --env-file ../.env python review_pr.py \
  --repo-url https://github.com/izzye84/feature-request-agent \
  --pr-url https://github.com/izzye84/feature-request-agent/pull/1 \
  --model-id claude-sonnet-5-5 --model-param reasoning_effort=high
```

Tests: `uv run pytest` (from `pr_review_gate/`). Without `--publish` the script only prints the decision (exit 0 = auto-approve, 3 = human review). Cloud agents started from the SDK are hidden from the default agent list in Cursor Web: use **Filter → Source → SDK**.

## Demo PRs

1. Typo or docs-only change → auto-approved
2. A diff that adds a fake credential → human review, blocker `secrets`
3. A change to auth or permission logic, or a large refactor → human review, blockers `auth` / `complexity`

## Build vs. buy

Cursor ships Bugbot and PR Routing & Approval as [Automations](https://cursor.com/docs/cloud-agent/automations) with no code. This gate is the SDK version: the approval rule is code you own and test, the model and its cost are chosen per repo, and every side effect is deterministic.

## Files

```
pr_review_gate/
├── review_pr.py        # Cursor SDK call (run_pr_review) + GitHub publishing + CLI
├── review_policy.py    # Prompt, verdict parsing, auto-approve policy (no I/O)
├── tests/              # Policy, model selection, SDK call and publishing (all faked)
├── pyproject.toml      # cursor-sdk only
└── uv.lock
.github/workflows/pr-review.yml   # Trigger (GitHub requires workflows here)
```

