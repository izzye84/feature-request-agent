import dagster as dg


@dg.asset
def asset_a() -> list:
    """Materialize a base list value."""
    return ["alpha", "beta"]


@dg.asset
def asset_b(asset_a: list) -> list:
    """Return asset_a's list with one additional value appended."""
    return asset_a + ["gamma"]
