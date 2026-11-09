import dagster as dg


@dg.asset
def asset_a() -> list:
    """Materialize a list of seed values."""
    return [1, 2, 3]


@dg.asset
def asset_b(asset_a: list) -> list:
    """Return asset_a with one additional value appended."""
    return asset_a + [4]
