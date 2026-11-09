import dagster as dg


@dg.asset
def asset_a() -> list:
    """Materialize an initial list value."""
    return [1, 2, 3]


@dg.asset
def asset_b(asset_a: list) -> list:
    """Append one value to asset_a's list."""
    return asset_a + [4]
