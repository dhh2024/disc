from dagster import Failure, OpExecutionContext, asset
from dagster_shell import execute_shell_command
from hereutil import here
from dagster_assets.commands import copy_to_gsheets, load_db_from_jsonl, copy_tables_to_columnstore, parse_dataset, sample_threads, create_parquets


@asset
def cmw_aria_table(context: OpExecutionContext) -> None:
    load_db_from_jsonl(context, "cmw")


@asset(deps=[cmw_aria_table])
def cmw_delta_comments_table(context: OpExecutionContext) -> None:
    _, ret = execute_shell_command("python src/jiemakel/create_delta_comments_table.py",
                                   cwd=str(here()),
                                   output_logging="STREAM", log=context.log)
    if ret != 0:
        raise Failure(f"Command failed with exit code {ret}")


@asset(deps=[cmw_aria_table])
def cmw_columnstore_table(context: OpExecutionContext) -> None:
    copy_tables_to_columnstore(context, "cmw_")


@asset(deps=[cmw_aria_table])
def cmw_sample_1(context: OpExecutionContext) -> None:
    sample_threads(context, "cmw_", 500, 1)


@asset(deps=[cmw_sample_1])
def cmw_sample_1_gsheets(context: OpExecutionContext) -> None:
    copy_to_gsheets(context, "cmw", 1)


@asset(deps=[cmw_aria_table])
def cmw_parquet(context: OpExecutionContext) -> None:
    create_parquets(context, ["cmw_submissions_a", "cmw_comments_a"])


@asset(deps=[cmw_delta_comments_table])
def cmw_delta_comments_parquet(context: OpExecutionContext) -> None:
    create_parquets(context, ["cmw_delta_comments_a"])


@asset(deps=[cmw_sample_1])
def cmw_sample_1_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["cmw_submissions_sample_1_a", "cmw_comments_sample_1_a"])


@asset(deps=[cmw_sample_1])
def cmw_comments_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "cmw_comments_sample_1")


@asset(deps=[cmw_sample_1])
def cmw_submissions_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "cmw_submissions_sample_1")
