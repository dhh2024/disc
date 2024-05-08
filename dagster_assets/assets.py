from dagster import Failure, OpExecutionContext, asset
from dagster_shell import execute_shell_command
from hereutil import here


def load_db_from_jsonl(context: OpExecutionContext, table_prefix: str) -> None:
    _, ret = execute_shell_command(f'python src/jiemakel/load_db_from_jsonl.py -tp {table_prefix} -c "data/input/{table_prefix}/comments/*.zst" -s "data/input/{table_prefix}/submissions/*.zst"',
                                   cwd=str(here()),
                                   output_logging="STREAM", log=context.log)
    if ret != 0:
        raise Failure(f"Command failed with exit code {ret}")


@asset
def eli5_aria_table(context: OpExecutionContext) -> None:
    load_db_from_jsonl(context, "eli5_")


@asset
def cmw_aria_table(context: OpExecutionContext) -> None:
    load_db_from_jsonl(context, "cmw_")


@asset
def aita_aria_table(context: OpExecutionContext) -> None:
    load_db_from_jsonl(context, "aita_")


@asset(deps=[cmw_aria_table])
def cmw_delta_comments_table(context: OpExecutionContext) -> None:
    _, ret = execute_shell_command("python src/jiemakel/create_delta_comments_table.py",
                                   cwd=str(here()),
                                   output_logging="STREAM", log=context.log)
    if ret != 0:
        raise Failure(f"Command failed with exit code {ret}")


def copy_tables_to_columnstore(context: OpExecutionContext, table_prefix: str) -> None:
    _, ret = execute_shell_command(f"python src/jiemakel/copy_tables_to_columnstore.py -tp {table_prefix}",
                                   cwd=str(here()),
                                   output_logging="STREAM", log=context.log)
    if ret != 0:
        raise Failure(f"Command failed with exit code {ret}")


@asset(deps=[eli5_aria_table])
def eli5_columnstore_table(context: OpExecutionContext) -> None:
    copy_tables_to_columnstore(context, "eli5_")


@asset(deps=[cmw_aria_table])
def cmw_columnstore_table(context: OpExecutionContext) -> None:
    copy_tables_to_columnstore(context, "cmw_")


@asset(deps=[aita_aria_table])
def aita_columnstore_table(context: OpExecutionContext) -> None:
    copy_tables_to_columnstore(context, "aita_")


def sample_threads(context: OpExecutionContext, table_prefix: str, submissions_sample_size: int, sample_number: int) -> None:
    _, ret = execute_shell_command(f"python src/jiemakel/sample_threads.py -tp {table_prefix} -n {submissions_sample_size} -s {sample_number}",
                                   cwd=str(here()),
                                   output_logging="STREAM", log=context.log)
    if ret != 0:
        raise Failure(f"Command failed with exit code {ret}")


@asset(deps=[eli5_aria_table])
def eli5_sample_1(context: OpExecutionContext) -> None:
    sample_threads(context, "eli5_", 1000, 1)


@asset(deps=[cmw_aria_table])
def cmw_sample_1(context: OpExecutionContext) -> None:
    sample_threads(context, "cmw_", 1000, 1)


@asset(deps=[aita_aria_table])
def aita_sample_1(context: OpExecutionContext) -> None:
    sample_threads(context, "aita_", 1000, 1)


def create_parquets(context: OpExecutionContext, tables: list[str]) -> None:
    _, ret = execute_shell_command(f"python src/jiemakel/create_parquets.py {' '.join(tables)}",
                                   cwd=str(here()),
                                   output_logging="STREAM", log=context.log)
    if ret != 0:
        raise Failure(f"Command failed with exit code {ret}")


@asset(deps=[eli5_aria_table])
def eli5_parquet(context: OpExecutionContext) -> None:
    create_parquets(context, ["eli5_submissions_a", "eli5_comments_a"])


@asset(deps=[eli5_sample_1])
def eli5_sample_1_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["eli5_submissions_sample_1_a", "eli5_comments_sample_1_a"])


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


@asset(deps=[aita_aria_table])
def aita_parquet(context: OpExecutionContext) -> None:
    create_parquets(context, ["aita_submissions_a", "aita_comments_a"])


@asset(deps=[aita_sample_1])
def aita_sample_1_parquet(context: OpExecutionContext) -> None:
    create_parquets(
        context, ["aita_submissions_sample_1_a", "aita_comments_sample_1_a"])
