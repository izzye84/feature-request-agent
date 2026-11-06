import json
from types import SimpleNamespace

import dagster as dg
import pytest

from dagster_agent_demo.defs.assets import ingest_requirements
from dagster_agent_demo.resources import GrokResource
from dagster_agent_demo.resources import grok as grok_module

SPEC = {
    "title": "Txn outliers",
    "objective": "Flag outlier transactions",
    "acceptance_criteria": ["works"],
    "data_assets_affected": ["transactions"],
    "complexity": "low",
    "estimated_hours": 2,
}


class FakeCompletions:
    def __init__(self):
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(SPEC)))],
            usage=SimpleNamespace(
                prompt_tokens=100,
                completion_tokens=50,
                total_tokens=150,
                prompt_tokens_details=SimpleNamespace(cached_tokens=10),
                completion_tokens_details=SimpleNamespace(reasoning_tokens=20),
            ),
        )


class FakeOpenAI:
    instances: list["FakeOpenAI"] = []

    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url
        self.chat = SimpleNamespace(completions=FakeCompletions())
        FakeOpenAI.instances.append(self)

    def close(self):
        pass


@pytest.fixture(autouse=True)
def fake_openai(monkeypatch):
    FakeOpenAI.instances.clear()
    monkeypatch.setattr(grok_module, "OpenAI", FakeOpenAI)


def test_ingest_requirements_runs_against_grok():
    result = dg.materialize(
        [ingest_requirements],
        resources={"grok": GrokResource(api_key="test-key")},
    )
    assert result.success

    client = FakeOpenAI.instances[0]
    assert client.base_url == "https://api.x.ai/v1"
    assert client.chat.completions.calls[0]["model"] == "grok-4.7"

    metadata = result.asset_materializations_for_node("ingest_requirements")[0].metadata
    assert metadata["spec"].value == SPEC
    assert metadata["tokens_used"].value == 150
    assert metadata["grok.calls"].value == 1
    assert metadata["grok.prompt_tokens"].value == 100
    assert metadata["grok.cached_prompt_tokens"].value == 10
    assert metadata["grok.reasoning_tokens"].value == 20


def test_get_client_does_not_stack_wrappers():
    @dg.asset
    def two_calls(context: dg.AssetExecutionContext, grok: GrokResource):
        for _ in range(2):
            with grok.get_client(context) as client:
                client.chat.completions.create(model="grok-4.7", messages=[])

    result = dg.materialize(
        [two_calls], resources={"grok": GrokResource(api_key="test-key")}
    )
    metadata = result.asset_materializations_for_node("two_calls")[0].metadata
    assert metadata["grok.calls"].value == 2
    assert metadata["grok.total_tokens"].value == 300
