from dagster import Failure, OpExecutionContext, asset
from dagster_shell import execute_shell_command
from hereutil import here
from dagster_assets.commands import copy_to_gsheets, load_db_from_jsonl, copy_tables_to_columnstore, parse_dataset, sample_threads, create_parquets


@asset
def stop_arguing_aria_table(context: OpExecutionContext) -> None:
    load_db_from_jsonl(context, "stop_arguing")


@asset(deps=[stop_arguing_aria_table])
def stop_arguing_columnstore_table(context: OpExecutionContext) -> None:
    copy_tables_to_columnstore(context, "stop_arguing_")


@asset(deps=[stop_arguing_aria_table])
def stop_arguing_stop_arguing_comments_table(context: OpExecutionContext) -> None:
    _, ret = execute_shell_command("python src/jiemakel/create_stop_arguing_comments_table.py",
                                   cwd=str(here()),
                                   output_logging="STREAM", log=context.log)
    if ret != 0:
        raise Failure(f"Command failed with exit code {ret}")


@asset(deps=[stop_arguing_aria_table])
def stop_arguing_sample_1(context: OpExecutionContext) -> None:
    sample_threads(context, "stop_arguing_", 500, 1)


@asset(deps=[stop_arguing_sample_1])
def stop_arguing_sample_1_gsheets(context: OpExecutionContext) -> None:
    copy_to_gsheets(context, "stop_arguing", 1)


@asset(deps=[stop_arguing_aria_table])
def stop_arguing_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["stop_arguing_submissions_a", "stop_arguing_comments_a"])


@asset(deps=[stop_arguing_sample_1])
def stop_arguing_sample_1_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["stop_arguing_submissions_sample_1_a", "stop_arguing_comments_sample_1_a"])


@asset(deps=[stop_arguing_sample_1])
def stop_arguing_comments_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "stop_arguing_comments_sample_1")


@asset(deps=[stop_arguing_sample_1])
def stop_arguing_submissions_sample_1_parse(context: OpExecutionContext) -> None:
    parse_dataset(context, "stop_arguing_submissions_sample_1")
