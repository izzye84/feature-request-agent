import dagster as dg
from dagster import AssetKey

from list_append import asset_a, asset_b


def test_asset_a_materializes_list():
    result = dg.materialize([asset_a])
    assert result.success
    assert result.output_for_node("asset_a") == ["alpha", "beta"]


def test_asset_b_depends_on_asset_a():
    assert asset_b.dependency_keys == {AssetKey("asset_a")}


def test_asset_b_appends_one_value():
    result = dg.materialize([asset_a, asset_b])
    assert result.success
    assert result.output_for_node("asset_b") == ["alpha", "beta", "gamma"]
