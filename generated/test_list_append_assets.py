import dagster as dg

from list_append_assets import asset_a, asset_b


def test_asset_a_materializes_list():
    result = dg.materialize([asset_a])
    assert result.success
    assert result.output_for_node("asset_a") == [1, 2, 3]


def test_asset_b_appends_one_value():
    result = dg.materialize([asset_a, asset_b])
    assert result.success
    assert result.output_for_node("asset_a") == [1, 2, 3]
    assert result.output_for_node("asset_b") == [1, 2, 3, 4]
