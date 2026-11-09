import statistics
from collections import defaultdict

import dagster as dg


def _category_stats(amounts: list[float]) -> tuple[float, float, int]:
    mean = statistics.mean(amounts)
    if len(amounts) < 2:
        return mean, 0.0, 0
    try:
        std = statistics.stdev(amounts)
    except statistics.StatisticsError:
        return mean, 0.0, 0
    if std == 0.0:
        return mean, std, 0
    threshold = 3 * std
    outlier_count = sum(1 for amount in amounts if abs(amount - mean) > threshold)
    return mean, std, outlier_count


def _summary_table(transactions) -> dict[str, list]:
    by_category: dict[object, list[float]] = defaultdict(list)
    for category, amount in zip(
        transactions["merchant_category"],
        transactions["amount"],
        strict=False,
    ):
        by_category[category].append(float(amount))

    categories: list[object] = []
    means: list[float] = []
    stds: list[float] = []
    outlier_counts: list[int] = []
    for category in sorted(by_category, key=str):
        mean, std, outlier_count = _category_stats(by_category[category])
        categories.append(category)
        means.append(mean)
        stds.append(std)
        outlier_counts.append(outlier_count)

    return {
        "merchant_category": categories,
        "mean": means,
        "std": stds,
        "outlier_count": outlier_counts,
    }


def _as_dataframe(table: dict[str, list]):
    try:
        import pandas as pd
    except ImportError:
        return table
    return pd.DataFrame(table)


@dg.asset
def merchant_category_transaction_summary(transactions):
    """Summarize transaction amounts by merchant category with outlier counts."""
    if len(transactions["merchant_category"]) == 0:
        raise ValueError("transactions must be non-empty")
    return _as_dataframe(_summary_table(transactions))
