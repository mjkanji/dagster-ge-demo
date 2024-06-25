import pandas as pd
from dagster import ConfigurableResource
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)


class GreatExpectationsResource(ConfigurableResource):
    def get_validator(self, asset_df: pd.DataFrame):
        project_config = DataContextConfig(
            store_backend_defaults=InMemoryStoreBackendDefaults()
        )
        data_context = EphemeralDataContext(project_config=project_config)
        data_source = data_context.sources.add_pandas(name="my_pandas_datasource")
        asset_name = "asset_check_df"
        suite_name = "asset_check_expectation_suite"
        data_asset = data_source.add_dataframe_asset(name=asset_name)
        batch_request = data_asset.build_batch_request(dataframe=asset_df)
        data_context.add_or_update_expectation_suite(suite_name)
        validator = data_context.get_validator(
            batch_request=batch_request, expectation_suite_name=suite_name
        )
        return validator
