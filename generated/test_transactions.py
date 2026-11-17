import math
import statistics

import dagster as dg
import pytest

from transactions import (
    DataFrame,
    merchant_category_transaction_summary,
    summarize_transactions,
)


@dg.asset
def transactions() -> DataFrame:
    grocery_rows = [
        {
            "transaction_id": f"t{i}",
            "amount": 10.0,
            "merchant_category": "grocery",
        }
        for i in range(1, 11)
    ]
    grocery_rows.append(
        {
            "transaction_id": "t11",
            "amount": 1000.0,
            "merchant_category": "grocery",
        }
    )
    return DataFrame(
        grocery_rows
        + [
            {"transaction_id": "t12", "amount": 5.0, "merchant_category": "fuel"},
            {"transaction_id": "t13", "amount": 20.0, "merchant_category": "retail"},
            {"transaction_id": "t14", "amount": 30.0, "merchant_category": "retail"},
        ]
    )


def test_summarize_computes_mean_std_and_outliers() -> None:
    amounts = [10.0] * 10 + [1000.0]
    groceries = DataFrame(
        [
            {
                "transaction_id": f"t{i}",
                "amount": amount,
                "merchant_category": "grocery",
            }
            for i, amount in enumerate(amounts, start=1)
        ]
    )
    summary = summarize_transactions(groceries)
    row = summary._records[0]
    assert row["merchant_category"] == "grocery"
    assert row["mean"] == statistics.mean(amounts)
    assert row["std"] == statistics.stdev(amounts)
    assert row["outlier_count"] == 1

    single_row = DataFrame(
        [{"transaction_id": "x", "amount": 7.5, "merchant_category": "fuel"}]
    )
    small = summarize_transactions(single_row)._records[0]
    assert math.isnan(small["std"])
    assert small["outlier_count"] == 0


def test_merchant_category_transaction_summary_materializes() -> None:
    result = dg.materialize([transactions, merchant_category_transaction_summary])
    assert result.success

    output = result.output_for_node("merchant_category_transaction_summary")
    assert isinstance(output, DataFrame)
    rows = output._records
    assert len(rows) == 3
    assert {frozenset(row) for row in rows} == {
        frozenset({"mean", "std", "outlier_count"})
    }

    fuel = next(r for r in rows if r["mean"] == 5.0)
    assert math.isnan(fuel["std"])
    assert fuel["outlier_count"] == 0

    grocery = next(r for r in rows if r["outlier_count"] == 1)
    assert grocery["mean"] == pytest.approx(100.0)