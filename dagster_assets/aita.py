from dagster import OpExecutionContext, asset
from dagster_assets.commands import copy_to_gsheets, load_db_from_jsonl, copy_tables_to_columnstore, parse_dataset, sample_threads, create_parquets


@asset
def aita_aria_table(context: OpExecutionContext) -> None:
    load_db_from_jsonl(context, "aita")


@asset(deps=[aita_aria_table])
def aita_columnstore_table(context: OpExecutionContext) -> None:
    copy_tables_to_columnstore(context, "aita_")


@asset(deps=[aita_aria_table])
def aita_sample_1(context: OpExecutionContext) -> None:
    sample_threads(context, "aita_", 500, 1)


@asset(deps=[aita_sample_1])
def aita_sample_1_gsheets(context: OpExecutionContext) -> None:
    copy_to_gsheets(context, "aita", 1)


@asset(deps=[aita_aria_table])
def aita_parquet(context: OpExecutionContext) -> None:
    create_parquets(context, ["aita_submissions_a", "aita_comments_a"])


@asset(deps=[aita_sample_1])
def aita_sample_1_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["aita_submissions_sample_1_a", "aita_comments_sample_1_a"])


@asset(deps=[aita_sample_1])
def aita_comments_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "aita_comments_sample_1")


@asset(deps=[aita_sample_1])
def aita_submissions_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "aita_submissions_sample_1")
