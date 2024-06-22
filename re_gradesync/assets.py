from dagster import AssetKey, SourceAsset, asset
import polars as pl
import pandas as pd
from dagster_deltalake import LocalConfig
from dagster_deltalake_pandas import DeltaLakePandasIOManager
from dagster import Definitions

# https://docs.dagster.io/integrations/deltalake/using-deltalake-with-dagster
@asset
def iris_dataset() -> pd.DataFrame:
    return pd.read_csv(
        "https://docs.dagster.io/assets/iris.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )

defs = Definitions(
    assets=[iris_dataset],
    resources={
        "io_manager": DeltaLakePandasIOManager(
            root_uri="path/to/deltalake",  # required
            storage_options=LocalConfig(),  # required
            schema="iris",  # optional, defaults to "public"
        )
    },
)


TEST_USERS_DATA = {
    'd0' : [
        {'user_id' : 'u1', 'fname':'Abe', 'date': '2024-02-20'},
        {'user_id' : 'u2', 'fname':'Alan', 'date': '2024-02-20'},
        {'user_id' : 'u3', 'fname':'Alice', 'date': '2024-02-20'},
        {'user_id' : 'u4', 'fname':'Amy', 'date': '2024-02-20'},
        {'user_id' : 'u5', 'fname':'Aaron', 'date': '2024-02-20'},
        {'user_id' : 'u6', 'fname':'Ashley', 'date': '2024-02-20'}
    ],
    'd1' : [
        {'user_id' : 'u1', 'fname':'Jon', 'date': '2024-02-21'},       # new row
        {'user_id' : 'u2', 'fname':'Jan', 'date': '2024-02-21'}        # new row
    ],
    'd2' : [
        {'user_id' : 'u1', 'fname':'Jonathan', 'date': '2024-02-21'},  # updated name
        {'user_id' : 'u2', 'fname':'Jan', 'date': '2024-02-21'},       # not changed
        {'user_id' : 'u3', 'fname':'Jan', 'date': '2024-02-21'},       # new row
    ],
    'd3' : [
        #                                      # deleted a row
        {'user_id' : 'u2', 'fname':'Jan', 'date': '2024-02-22'},   # updated date
        {'user_id' : 'u3', 'fname':'Jen', 'date': '2024-02-22'},   # updated name
        {'user_id' : 'u4', 'fname':'Jack', 'date': '2024-02-22'},  # new row
        {'user_id' : 'u5', 'fname':'Jill', 'date': '2024-02-22'},  # new row
    ],
    'd4' : [
        {'user_id' : 'u6', 'fname':'Jamie', 'date': '2024-02-22'},
        {'user_id' : 'u7', 'fname':'Janie', 'date': '2024-02-22'}, 
        {'user_id' : 'u6', 'fname':'James', 'date': '2024-02-22'}  # duplicate row within same batch, last one wins
    ]    
}

TEST_ASSIGNMENTS_DATA = {
    'd0' : [
        {'assignment_id': 'a1', 'class_id': 'c1', 'name': 'math sheet1'},
        {'assignment_id': 'a2', 'class_id': 'c1', 'name': 'math sheet2'},
    ],
}

TEST_SUBMISSIONS_DATA = {
    'd0' : [
        {'submission_id': 's1', 'assignment_id': 'a1', 'user_id': 'u1', 'score': 92},
        {'submission_id': 's2', 'assignment_id': 'a1', 'user_id': 'u2', 'score': 90},
        {'submission_id': 's3', 'assignment_id': 'a1', 'user_id': 'u3', 'score': 81},
    ],
}


@asset(group_name='gradesync')
def m365_users() -> pl.DataFrame:
    df = pl.DataFrame(TEST_USERS_DATA['d0'])
    return df

@asset(group_name='gradesync')
def m365_assignments() -> pl.DataFrame:
    df = pl.DataFrame(TEST_ASSIGNMENTS_DATA['d0'])
    return df

@asset(group_name='gradesync')
def m365_submissions() -> pl.DataFrame:
    df = pl.DataFrame(TEST_SUBMISSIONS_DATA['d0'])
    return df

@asset(group_name='uls')
def uls_users(m365_users: pl.DataFrame) -> pl.DataFrame:
    df = m365_users.rename({'fname': 'first_name'})
    return df