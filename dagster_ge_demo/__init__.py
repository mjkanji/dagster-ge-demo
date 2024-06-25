import numpy as np
import pandas as pd
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    Definitions,
    asset,
    asset_check,
    multi_asset_check,
)
from dagster._core.definitions.assets import AssetsDefinition
from dagster._core.definitions.events import CoercibleToAssetKey
from dagster._core.definitions.source_asset import SourceAsset
from great_expectations.core.expectation_configuration import ExpectationConfiguration

from dagster_ge_demo.resources import GreatExpectationsResource


@asset
def titanic():
    titanic_df = pd.read_csv(
        "https://github.com/datasciencedojo/datasets/raw/master/titanic.csv"
    )
    titanic_df.to_csv("titanic.csv", index=False)


@asset_check(asset=titanic, blocking=True)
def target_has_no_nulls(context: AssetCheckExecutionContext):
    titanic_df = pd.read_csv("titanic.csv")
    null_count = titanic_df["Survived"].isna().sum()

    return AssetCheckResult(
        passed=bool(null_count == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "null_count": int(null_count),
        },
    )


@asset_check(asset=titanic, blocking=True)
def ge_target_has_no_nulls(
    context: AssetCheckExecutionContext, ge_resource: GreatExpectationsResource
):
    titanic_df = pd.read_csv("titanic.csv")
    validator = ge_resource.get_validator(titanic_df)
    validation_result = validator.expect_column_values_to_not_be_null(column="Survived")

    return AssetCheckResult(
        passed=validation_result.success,
        severity=AssetCheckSeverity.ERROR,
        metadata=validation_result.result,
    )


###### MULTI ASSET CHECK ###########


@multi_asset_check(
    specs=[
        AssetCheckSpec(name="multicheck_target_has_no_nulls", asset=titanic),
        AssetCheckSpec(name="multicheck_target_has_valid_values", asset=titanic),
    ]
)
def ge_multiple_checks(
    context: AssetCheckExecutionContext, ge_resource: GreatExpectationsResource
):
    titanic_df = pd.read_csv("titanic.csv")
    validator = ge_resource.get_validator(titanic_df)

    validation_result = validator.expect_column_values_to_not_be_null(column="Survived")
    yield AssetCheckResult(
        passed=validation_result.success,
        severity=AssetCheckSeverity.ERROR,
        metadata=validation_result.result,
        check_name="multicheck_target_has_no_nulls",
    )

    validation_result = validator.expect_column_values_to_be_in_set(
        column="Survived", value_set={0, 1}
    )
    yield AssetCheckResult(
        passed=validation_result.success,
        severity=AssetCheckSeverity.ERROR,
        metadata=validation_result.result,
        check_name="multicheck_target_has_valid_values",
    )


####### FACTORY PATTERN #######


def make_ge_asset_check(
    expectation_name: str,
    expectation_config: ExpectationConfiguration,
    asset: CoercibleToAssetKey | AssetsDefinition | SourceAsset,
):
    @asset_check(
        asset=asset,
        name=expectation_name,
        blocking=True,
        compute_kind="great_expectations",
    )
    def _asset_check(
        context: AssetCheckExecutionContext, ge_resource: GreatExpectationsResource
    ):
        titanic_df = pd.read_csv("titanic.csv")
        validator = ge_resource.get_validator(titanic_df)
        validation_result = expectation_config.validate(validator)

        return AssetCheckResult(
            passed=validation_result.success,  # type: ignore
            severity=AssetCheckSeverity.ERROR,
            metadata=validation_result.result,
        )

    return _asset_check


specs = {
    "factory_target_has_no_nulls": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "Survived"},
    ),
    "factory_target_has_valid_values": ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "Survived", "value_set": {0, 1}},
    ),
}

titanic_checks = [make_ge_asset_check(k, v, titanic) for k, v in specs.items()]

defs = Definitions(
    assets=[titanic],
    asset_checks=[
        target_has_no_nulls,
        ge_target_has_no_nulls,
        ge_multiple_checks,
        *titanic_checks,
    ],
    resources={"ge_resource": GreatExpectationsResource()},
)
