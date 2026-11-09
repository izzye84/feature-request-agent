"""Merchant category transaction summary asset (stdlib-only; DataFrame-compatible I/O)."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict

import dagster as dg

_REQUIRED_COLUMNS = frozenset({"transaction_id", "amount", "merchant_category"})
_SUMMARY_COLUMNS = ("merchant_category", "mean", "std", "outlier_count")


class SummaryDataFrame:
    """Columnar summary table with the same column access pattern as pandas."""

    __slots__ = ("_data", "columns")

    def __init__(self, data: dict[str, list]) -> None:
        self._data = data
        self.columns = list(data.keys())

    def __getitem__(self, key: str) -> list:
        return self._data[key]

    def __len__(self) -> int:
        if not self._data:
            return 0
        return len(next(iter(self._data.values())))


def _validate_transactions(transactions) -> None:
    columns = getattr(transactions, "columns", None)
    if columns is None:
        msg = "transactions must be a DataFrame-like object with a columns attribute"
        raise TypeError(msg)
    missing = _REQUIRED_COLUMNS - set(columns)
    if missing:
        msg = f"transactions missing required columns: {sorted(missing)}"
        raise ValueError(msg)


def _category_stats(amounts: list[float]) -> tuple[float, float, int]:
    mean = statistics.fmean(amounts)
    outlier_count = 0
    if len(amounts) < 2:
        std = math.nan
    else:
        std = statistics.stdev(amounts)
        if not math.isnan(std) and std > 0:
            threshold = 3 * std
            outlier_count = sum(1 for a in amounts if abs(a - mean) > threshold)
    return mean, std, outlier_count


@dg.asset
def merchant_category_transaction_summary(transactions) -> SummaryDataFrame:
    """Summarize transaction amounts by merchant category with mean, std, and outlier counts."""
    _validate_transactions(transactions)
    by_category: dict[str, list[float]] = defaultdict(list)
    for amount, category in zip(
        transactions["amount"],
        transactions["merchant_category"],
        strict=True,
    ):
        by_category[str(category)].append(float(amount))

    rows = sorted(by_category.items(), key=lambda item: item[0])
    summary: dict[str, list] = {col: [] for col in _SUMMARY_COLUMNS}
    for category, amounts in rows:
        mean, std, outlier_count = _category_stats(amounts)
        summary["merchant_category"].append(category)
        summary["mean"].append(mean)
        summary["std"].append(std)
        summary["outlier_count"].append(outlier_count)
    return SummaryDataFrame(summary)
