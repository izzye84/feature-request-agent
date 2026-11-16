import dagster as dg

#intentionally not importing pandas to demonstrate auto CI fixes

@dg.asset
def my_dataframe() -> pd.DataFrame:
    my_list = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]
    return pd.DataFrame(my_list)