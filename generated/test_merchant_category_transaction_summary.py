import statistics

import dagster as dg

from merchant_category_transaction_summary import merchant_category_transaction_summary


class DataFrame:
    """Columnar table matching the pandas subset this asset uses."""

    def __init__(self, data: dict[str, list]):
        self._data = {key: list(values) for key, values in data.items()}

    def __getitem__(self, key: str):
        return self._data[key]

    def __len__(self) -> int:
        return len(self._data["merchant_category"])


@dg.asset
def transactions() -> DataFrame:
    return DataFrame(
        {
            "transaction_id": ["t1", "t2", "t3", "t4", "t5", "t6"],
            "amount": [10.0, 12.0, 11.0, 100.0, 10.0, 10.0],
            "merchant_category": ["grocery", "grocery", "grocery", "grocery", "fuel", "fuel"],
        }
    )


def _row(summary: dict[str, list], category: str) -> dict:
    index = summary["merchant_category"].index(category)
    return {
        "mean": summary["mean"][index],
        "std": summary["std"][index],
        "outlier_count": summary["outlier_count"][index],
    }


def test_merchant_category_transaction_summary_materializes():
    result = dg.materialize([transactions, merchant_category_transaction_summary])
    assert result.success

    table = result.output_for_node("merchant_category_transaction_summary")

    assert set(table.keys()) == {
        "merchant_category",
        "mean",
        "std",
        "outlier_count",
    }
    assert len(table["merchant_category"]) == 2

    grocery_amounts = [10.0, 12.0, 11.0, 100.0]
    grocery_mean = statistics.mean(grocery_amounts)
    grocery_std = statistics.stdev(grocery_amounts)
    grocery_outliers = sum(
        1 for amount in grocery_amounts if abs(amount - grocery_mean) > 3 * grocery_std
    )
    grocery = _row(table, "grocery")
    assert grocery["mean"] == grocery_mean
    assert grocery["std"] == grocery_std
    assert grocery["outlier_count"] == grocery_outliers

    fuel = _row(table, "fuel")
    assert fuel["mean"] == 10.0
    assert fuel["std"] == 0.0
    assert fuel["outlier_count"] == 0


def test_single_row_category_has_zero_outliers():
    @dg.asset(name="transactions")
    def solo_transactions() -> DataFrame:
        return DataFrame(
            {
                "transaction_id": ["t1"],
                "amount": [42.0],
                "merchant_category": ["retail"],
            }
        )

    result = dg.materialize([solo_transactions, merchant_category_transaction_summary])
    assert result.success
    table = result.output_for_node("merchant_category_transaction_summary")
    assert table["outlier_count"] == [0]
    assert table["std"] == [0.0]
