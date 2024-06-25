from dagster import Definitions, load_assets_from_modules
from dagster_deltalake import LocalConfig
from dagster_deltalake_pandas import DeltaLakePandasIOManager

from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": DeltaLakePandasIOManager(
            root_uri='lakehouse', # 'lakehouse/tables',  # required
            storage_options=LocalConfig(),  # required
            schema='main',  # optional, defaults to "public"
        )
    },    
)
