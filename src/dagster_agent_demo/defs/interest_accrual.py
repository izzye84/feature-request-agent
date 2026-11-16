"""Daily deposit interest accrual for commercial DDA accounts.

Posts accrued interest to the deposit GL for an inclusive posting-date
window. Balances are a warehouse extract (in-memory for this project).
"""

from datetime import date
from decimal import ROUND_HALF_EVEN, Decimal

import dagster as dg

_CENTS = Decimal("0.01")
_ACTUAL_365 = Decimal("365")

# Warehouse extract of commercial DDA ledger balances used for accrual.
LEDGER_BALANCES: tuple[dict[str, str], ...] = (
    {
        "account_id": "DDA-4401827",
        "product": "commercial_dda",
        "posting_date": "2024-01-15",
        "ledger_balance_usd": "10000000.00",
        "annual_rate": "0.0511",
    },
    {
        "account_id": "DDA-4401827",
        "product": "commercial_dda",
        "posting_date": "2024-01-31",
        "ledger_balance_usd": "25000000.00",
        "annual_rate": "0.0511",
    },
)


class DailyInterestConfig(dg.Config):
    posting_date_start: str = "2024-01-01"
    posting_date_end: str = "2024-01-31"


def daily_accrual(ledger_balance_usd: Decimal, annual_rate: Decimal) -> Decimal:
    """Actual/365 daily interest, banker's rounding to the cent."""
    return (ledger_balance_usd * annual_rate / _ACTUAL_365).quantize(
        _CENTS, rounding=ROUND_HALF_EVEN
    )


@dg.asset(
    group_name="treasury",
    kinds={"python"},
    description=(
        "Accrues daily interest on commercial DDA ledger balances for a "
        "posting-date window inclusive of posting_date_start and posting_date_end. "
        "Output is the amount to post to the deposit GL."
    ),
    code_version="v1",
    owners=["team:treasury"],
    tags={"domain": "deposits", "layer": "finance"},
)
def daily_interest_accrual(
    context: dg.AssetExecutionContext,
    config: DailyInterestConfig,
) -> dg.MaterializeResult:
    start = date.fromisoformat(config.posting_date_start)
    end = date.fromisoformat(config.posting_date_end)

    accrued = Decimal("0.00")
    posting_days: set[date] = set()
    accounts: set[str] = set()
    lines: list[dict[str, str]] = []

    for row in LEDGER_BALANCES:
        posting_date = date.fromisoformat(row["posting_date"])
        if start <= posting_date <= end:
            interest = daily_accrual(
                Decimal(row["ledger_balance_usd"]),
                Decimal(row["annual_rate"]),
            )
            accrued += interest
            posting_days.add(posting_date)
            accounts.add(row["account_id"])
            lines.append(
                {
                    "account_id": row["account_id"],
                    "posting_date": row["posting_date"],
                    "accrued_interest_usd": str(interest),
                }
            )

    context.log.info(
        f"Accrued {accrued} USD across {len(posting_days)} posting days "
        f"for {len(accounts)} accounts "
        f"({config.posting_date_start} to {config.posting_date_end})."
    )

    return dg.MaterializeResult(
        metadata={
            "accrued_interest_usd": dg.MetadataValue.float(float(accrued)),
            "posting_days": dg.MetadataValue.int(len(posting_days)),
            "account_count": dg.MetadataValue.int(len(accounts)),
            "posting_date_start": dg.MetadataValue.text(config.posting_date_start),
            "posting_date_end": dg.MetadataValue.text(config.posting_date_end),
        },
        value={
            "accrued_interest_usd": str(accrued),
            "posting_days": len(posting_days),
            "account_count": len(accounts),
            "lines": lines,
        },
    )
