from dagster import OpExecutionContext, asset
from dagster_assets.commands import copy_to_gsheets, load_db_from_jsonl, copy_tables_to_columnstore, parse_dataset, sample_threads, create_parquets


@asset
def unpopularopinion_aria_table(context: OpExecutionContext) -> None:
    return
    load_db_from_jsonl(context, "unpopularopinion")


@asset(deps=[unpopularopinion_aria_table])
def unpopularopinion_columnstore_table(context: OpExecutionContext) -> None:
    copy_tables_to_columnstore(context, "unpopularopinion_")


@asset(deps=[unpopularopinion_aria_table])
def unpopularopinion_sample_1(context: OpExecutionContext) -> None:
    sample_threads(context, "unpopularopinion_", 500, 1)


@asset(deps=[unpopularopinion_sample_1])
def unpopularopinion_sample_1_gsheets(context: OpExecutionContext) -> None:
    copy_to_gsheets(context, "unpopularopinion", 1)


@asset(deps=[unpopularopinion_aria_table])
def unpopularopinion_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["unpopularopinion_submissions_a", "unpopularopinion_comments_a"])


@asset(deps=[unpopularopinion_sample_1])
def unpopularopinion_sample_1_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["unpopularopinion_submissions_sample_1_a", "unpopularopinion_comments_sample_1_a"])


@asset(deps=[unpopularopinion_sample_1])
def unpopularopinion_comments_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "unpopularopinion_comments_sample_1")


@asset(deps=[unpopularopinion_sample_1])
def unpopularopinion_submissions_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "unpopularopinion_submissions_sample_1")
