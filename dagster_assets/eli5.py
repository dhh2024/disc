from dagster import OpExecutionContext, asset
from dagster_assets.commands import load_db_from_jsonl, copy_tables_to_columnstore, sample_threads, create_parquets


@asset
def eli5_aria_table(context: OpExecutionContext) -> None:
    load_db_from_jsonl(context, "eli5_")


@asset(deps=[eli5_aria_table])
def eli5_columnstore_table(context: OpExecutionContext) -> None:
    copy_tables_to_columnstore(context, "eli5_")


@asset(deps=[eli5_aria_table])
def eli5_sample_1(context: OpExecutionContext) -> None:
    sample_threads(context, "eli5_", 1000, 1)


@asset(deps=[eli5_aria_table])
def eli5_parquet(context: OpExecutionContext) -> None:
    create_parquets(context, ["eli5_submissions_a", "eli5_comments_a"])


@asset(deps=[eli5_sample_1])
def eli5_sample_1_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["eli5_submissions_sample_1_a", "eli5_comments_sample_1_a"])
