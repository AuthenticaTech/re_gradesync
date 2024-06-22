import pandas as pd
from dagster import AssetKey, SourceAsset, asset
import polars as pl


@asset(group_name='gradesync')
def m365_users() -> pl.DataFrame:
    df = pl.DataFrame({
        "user_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })
    return df

@asset(group_name='gradesync')
def m365_classes() -> pl.DataFrame:
    df = pl.DataFrame({
        "class_id": [1, 2, 3],
        "name": ["Math", "Science", "History"]
    })
    return df