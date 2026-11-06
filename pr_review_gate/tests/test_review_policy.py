from review_policy import apply_policy, extract_json


def test_docs_pr_auto_approves():
    decision = apply_policy(
        {
            "verdict": "auto_approve",
            "complexity": "low",
            "touches_auth": False,
            "secrets_findings": [],
            "auth_findings": [],
            "summary": "Fix a typo in the README",
            "rationale": "docs only",
        }
    )
    assert decision["route"] == "auto_approve"
    assert decision["blockers"] == []


def test_secrets_force_human_even_if_model_approves():
    decision = apply_policy(
        {
            "verdict": "auto_approve",
            "complexity": "trivial",
            "touches_auth": False,
            "secrets_findings": ["config.py:12 — hardcoded AWS key"],
            "auth_findings": [],
        }
    )
    assert decision["route"] == "human_review"
    assert "secrets" in decision["blockers"]


def test_auth_rewrite_is_human_review():
    decision = apply_policy(
        {
            "verdict": "human_review",
            "complexity": "high",
            "touches_auth": True,
            "secrets_findings": [],
            "auth_findings": ["auth/sso.py — new JWT validation path"],
        }
    )
    assert decision["route"] == "human_review"
    assert "auth" in decision["blockers"]
    assert "complexity" in decision["blockers"]


def test_extract_json_from_fenced_response():
    payload = extract_json(
        'Here you go:\n```json\n{"verdict": "auto_approve", "complexity": "low"}\n```\n'
    )
    assert payload["verdict"] == "auto_approve"


def test_extract_json_ignores_trailing_prose():
    payload = extract_json('{"verdict": "human_review", "complexity": "high"}\nLet me know!')
    assert payload["complexity"] == "high"


def test_model_recommending_human_review_is_respected():
    decision = apply_policy(
        {"verdict": "human_review", "complexity": "low", "touches_auth": False,
         "secrets_findings": [], "auth_findings": []}
    )
    assert decision["route"] == "human_review"
    assert decision["blockers"] == ["model_recommendation"]
