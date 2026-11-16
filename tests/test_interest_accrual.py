import dagster as dg

from dagster_agent_demo.defs.interest_accrual import daily_interest_accrual


def test_mid_month_window_accrues_the_fifteenth() -> None:
    result = dg.materialize(
        [daily_interest_accrual],
        run_config=dg.RunConfig(
            ops={
                "daily_interest_accrual": {
                    "config": {
                        "posting_date_start": "2024-01-01",
                        "posting_date_end": "2024-01-20",
                    }
                }
            }
        ),
    )

    assert result.success
    output = result.output_for_node("daily_interest_accrual")
    assert output["accrued_interest_usd"] == "1400.00"
    assert output["posting_days"] == 1
    assert output["account_count"] == 1


def test_month_end_window_includes_the_last_day() -> None:
    result = dg.materialize(
        [daily_interest_accrual],
        run_config=dg.RunConfig(
            ops={
                "daily_interest_accrual": {
                    "config": {
                        "posting_date_start": "2024-01-01",
                        "posting_date_end": "2024-01-31",
                    }
                }
            }
        ),
    )

    assert result.success
    output = result.output_for_node("daily_interest_accrual")
    assert output["accrued_interest_usd"] == "4900.00"
    assert output["posting_days"] == 2
    assert output["account_count"] == 1
    posted = {
        line["posting_date"]: line["accrued_interest_usd"] for line in output["lines"]
    }
    assert posted["2024-01-15"] == "1400.00"
    assert posted["2024-01-31"] == "3500.00"
