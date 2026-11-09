import dagster as dg

from dagster_agent_demo.defs.generated.asset_a_b import asset_a, asset_b


def test_asset_b_appends_one_value():
    result = dg.materialize([asset_a, asset_b])
    assert result.success
    values = result.output_for_node("asset_b")
    assert values == [1, 2, 3, 4]


def test_asset_a_materializes_list():
    result = dg.materialize([asset_a])
    assert result.success
    assert result.output_for_node("asset_a") == [1, 2, 3]
