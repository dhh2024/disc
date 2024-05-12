from dagster import OpExecutionContext, asset
from dagster_assets.commands import copy_to_gsheets, load_db_from_jsonl, copy_tables_to_columnstore, parse_dataset, sample_threads, create_parquets


@asset
def random_sample_aria_table(context: OpExecutionContext) -> None:
    load_db_from_jsonl(context, "random_sample")


@asset(deps=[random_sample_aria_table])
def random_sample_columnstore_table(context: OpExecutionContext) -> None:
    copy_tables_to_columnstore(context, "random_sample_")


@asset(deps=[random_sample_aria_table])
def random_sample_sample_1(context: OpExecutionContext) -> None:
    sample_threads(context, "random_sample_", 500, 1)


@asset(deps=[random_sample_sample_1])
def random_sample_sample_1_gsheets(context: OpExecutionContext) -> None:
    copy_to_gsheets(context, "random_sample", 1)


@asset(deps=[random_sample_aria_table])
def random_sample_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["random_sample_submissions_a", "random_sample_comments_a"])


@asset(deps=[random_sample_sample_1])
def random_sample_sample_1_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["random_sample_submissions_sample_1_a", "random_sample_comments_sample_1_a"])


@asset(deps=[random_sample_sample_1])
def random_sample_comments_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "random_sample_comments_sample_1")


@asset(deps=[random_sample_sample_1])
def random_sample_submissions_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "random_sample_submissions_sample_1")
