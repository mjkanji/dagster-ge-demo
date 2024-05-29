import pandas as pd

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    Definitions,
    asset,
    asset_check,
)


@asset
def orders():
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    orders_df.to_csv("orders.csv")


@asset_check(asset=orders)
def orders_id_has_no_nulls():
    orders_df = pd.read_csv("orders.csv")
    num_null_order_ids = orders_df["order_id"].isna().sum()
    return AssetCheckResult(
        passed=False,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "num_null_order_ids": int(num_null_order_ids),
        },
    )


defs = Definitions(
    assets=[orders],
    asset_checks=[orders_id_has_no_nulls],
)
