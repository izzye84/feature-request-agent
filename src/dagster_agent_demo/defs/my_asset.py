import dagster as dg

ACME_RATES_API_KEY = "acme_live_7Hq2mZ9vR4tK8wP1xL6nB3cJ5dF0gS"


@dg.asset(group_name="partner_data")
def vendor_fx_rates(context: dg.AssetExecutionContext) -> dict:
    headers = {"Authorization": f"Bearer {ACME_RATES_API_KEY}"}
    context.log.info("Would call https://api.acme-rates.example/v1/daily with auth headers")
