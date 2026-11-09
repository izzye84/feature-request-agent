import math
import statistics
import sys
from pathlib import Path

import dagster as dg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from merchant_category_transaction_summary import merchant_category_transaction_summary


class DataFrame:
    """Minimal pandas DataFrame stand-in for tests (pandas is not a project dependency)."""

    def __init__(self, data: dict[str, list]) -> None:
        self._data = data
        self.columns = list(data.keys())

    def __getitem__(self, key: str) -> list:
        return self._data[key]

    def __len__(self) -> int:
        return len(self._data["amount"])


def _transactions(rows: list[tuple[str, float, str]]) -> DataFrame:
    return DataFrame(
        {
            "transaction_id": [r[0] for r in rows],
            "amount": [r[1] for r in rows],
            "merchant_category": [r[2] for r in rows],
        }
    )


@dg.asset
def transactions() -> DataFrame:
    groceries = [(f"g{i}", 100.0, "groceries") for i in range(20)]
    groceries.append(("g-outlier", 10_000.0, "groceries"))
    gas = [("gas1", 50.0, "gas"), ("gas2", 60.0, "gas"), ("gas3", 55.0, "gas")]
    return _transactions(groceries + gas)


def test_merchant_category_transaction_summary_materializes():
    result = dg.materialize([transactions, merchant_category_transaction_summary])
    assert result.success

    summary = result.output_for_node("merchant_category_transaction_summary")
    assert list(summary.columns) == [
        "merchant_category",
        "mean",
        "std",
        "outlier_count",
    ]

    by_cat = {
        cat: {
            "mean": summary["mean"][i],
            "std": summary["std"][i],
            "outlier_count": summary["outlier_count"][i],
        }
        for i, cat in enumerate(summary["merchant_category"])
    }

    gas_amounts = [50.0, 60.0, 55.0]
    assert by_cat["gas"]["mean"] == statistics.fmean(gas_amounts)
    assert by_cat["gas"]["std"] == statistics.stdev(gas_amounts)
    assert by_cat["gas"]["outlier_count"] == 0

    grocery_amounts = [100.0] * 20 + [10_000.0]
    assert by_cat["groceries"]["mean"] == statistics.fmean(grocery_amounts)
    assert by_cat["groceries"]["std"] == statistics.stdev(grocery_amounts)
    assert by_cat["groceries"]["outlier_count"] == 1


def test_missing_columns_raises():
    bad = DataFrame({"transaction_id": ["a"], "amount": [1.0]})
    with pytest.raises(ValueError, match="merchant_category"):
        merchant_category_transaction_summary(bad)


def test_single_row_category_has_nan_std_and_no_outliers():
    single = _transactions([("t1", 42.0, "solo")])
    summary = merchant_category_transaction_summary(single)
    assert summary["merchant_category"] == ["solo"]
    assert summary["mean"] == [42.0]
    assert math.isnan(summary["std"][0])
    assert summary["outlier_count"] == [0]
