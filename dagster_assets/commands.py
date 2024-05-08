from dagster import Failure, OpExecutionContext, asset
from dagster_shell import execute_shell_command
from hereutil import here


def load_db_from_jsonl(context: OpExecutionContext, table_prefix: str) -> None:
    _, ret = execute_shell_command(f'python src/jiemakel/load_db_from_jsonl.py -tp {table_prefix} -c "data/input/{table_prefix}/comments/*.zst" -s "data/input/{table_prefix}/submissions/*.zst"',
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


def sample_threads(context: OpExecutionContext, table_prefix: str, submissions_sample_size: int, sample_number: int) -> None:
    _, ret = execute_shell_command(f"python src/jiemakel/sample_threads.py -tp {table_prefix} -n {submissions_sample_size} -s {sample_number} -f",
                                   cwd=str(here()),
                                   output_logging="STREAM", log=context.log)
    if ret != 0:
        raise Failure(f"Command failed with exit code {ret}")


def create_parquets(context: OpExecutionContext, tables: list[str]) -> None:
    _, ret = execute_shell_command(f"python src/jiemakel/create_parquets.py {' '.join(tables)}",
                                   cwd=str(here()),
                                   output_logging="STREAM", log=context.log)
    if ret != 0:
        raise Failure(f"Command failed with exit code {ret}")


def copy_to_gsheets(context: OpExecutionContext, table_prefix: str, sample_number: int) -> None:
    _, ret = execute_shell_command(f"Rscript src/jiemakel/copy_to_gsheets.R -p {table_prefix} -s {sample_number}",
                                   cwd=str(here()),
                                   output_logging="STREAM", log=context.log)
    if ret != 0:
        raise Failure(f"Command failed with exit code {ret}")
