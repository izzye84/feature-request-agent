"""Merchant category transaction summary."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from typing import Any, Iterator

import dagster as dg


class DataFrame:
    """Minimal columnar table for transaction inputs and summaries."""

    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = list(records)

    def __len__(self) -> int:
        return len(self._records)

    def groupby(self, column: str) -> Iterator[tuple[Any, list[float]]]:
        groups: dict[Any, list[float]] = defaultdict(list)
        for row in self._records:
            groups[row[column]].append(float(row["amount"]))
        for key in sorted(groups, key=lambda k: str(k)):
            yield key, groups[key]


def _category_std(amounts: list[float]) -> float:
    if len(amounts) < 2:
        return math.nan
    return statistics.stdev(amounts)


def _outlier_count(amounts: list[float], mean: float, std: float) -> int:
    if len(amounts) < 2 or math.isnan(std) or std == 0:
        return 0
    threshold = 3 * std
    return sum(1 for amount in amounts if abs(amount - mean) > threshold)


def summarize_transactions(transactions: DataFrame) -> DataFrame:
    rows: list[dict[str, Any]] = []
    for category, amounts in transactions.groupby("merchant_category"):
        mean = statistics.mean(amounts)
        std = _category_std(amounts)
        rows.append(
            {
                "merchant_category": category,
                "mean": mean,
                "std": std,
                "outlier_count": _outlier_count(amounts, mean, std),
            }
        )
    return DataFrame(rows)


@dg.asset
def merchant_category_transaction_summary(transactions: DataFrame) -> DataFrame:
    """Summarize transaction amounts by merchant category."""
    summary = summarize_transactions(transactions)
    return DataFrame(
        [
            {
                "mean": row["mean"],
                "std": row["std"],
                "outlier_count": row["outlier_count"],
            }
            for row in summary._records
        ]
    )