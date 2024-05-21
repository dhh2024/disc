from dagster import OpExecutionContext, asset
from dagster_assets.commands import copy_to_gsheets, load_db_from_jsonl, copy_tables_to_columnstore, parse_dataset, sample_threads, create_parquets


@asset
def best_of_reddit_aria_table(context: OpExecutionContext) -> None:
    load_db_from_jsonl(context, "best_of_reddit")


@asset(deps=[best_of_reddit_aria_table])
def best_of_reddit_columnstore_table(context: OpExecutionContext) -> None:
    copy_tables_to_columnstore(context, "best_of_reddit_")


@asset(deps=[best_of_reddit_aria_table])
def best_of_reddit_sample_1(context: OpExecutionContext) -> None:
    sample_threads(context, "best_of_reddit_", 500, 1)


@asset(deps=[best_of_reddit_sample_1])
def best_of_reddit_sample_1_gsheets(context: OpExecutionContext) -> None:
    copy_to_gsheets(context, "best_of_reddit", 1)


@asset(deps=[best_of_reddit_aria_table])
def best_of_reddit_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["best_of_reddit_submissions_a", "best_of_reddit_comments_a"])


@asset(deps=[best_of_reddit_sample_1])
def best_of_reddit_sample_1_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["best_of_reddit_submissions_sample_1_a", "best_of_reddit_comments_sample_1_a"])


@asset(deps=[best_of_reddit_sample_1])
def best_of_reddit_comments_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "best_of_reddit_comments_sample_1")


@asset(deps=[best_of_reddit_sample_1])
def best_of_reddit_submissions_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "best_of_reddit_submissions_sample_1")
