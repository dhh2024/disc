# This code creates smaller sample tables from the original tables in the database.

import click
from hereutil import here, add_to_sys_path
import pandas as pd
from sqlalchemy import text
from contextlib import closing
add_to_sys_path(here())
from src.common_basis import get_db_connection  # noqa
eng, con = get_db_connection()


@click.option('-tp', '--table_prefix', required=True, help="table prefix")
@click.option('-n', '--submissions', required=True, help="number of submissions to sample")
@click.option('-s', '--sample-number', required=True, help="sample number")
@click.option('-f', '--force', is_flag=True, help="allow overwriting an existing sample", default=False)
@click.command
def sample_threads(table_prefix: str, submissions: int, sample_number: int, force: bool):
    if force:
        con.execute(text(f"DROP TABLE IF EXISTS {
                    table_prefix}submissions_sample_{sample_number}_a"))
    con.execute(text(
        f"CREATE TABLE {table_prefix}submissions_sample_{sample_number}_a LIKE {table_prefix}submissions_a"))
    con.execute(
        text(f"ALTER TABLE {table_prefix}submissions_sample_{sample_number}_a DISABLE KEYS"))
    if force:
        con.execute(text(f"DROP TABLE IF EXISTS {
                    table_prefix}comments_sample_{sample_number}_a"))
    con.execute(text(
        f"CREATE TABLE {table_prefix}comments_sample_{sample_number}_a LIKE {table_prefix}comments_a"))
    con.execute(text(f"ALTER TABLE {table_prefix}comments_sample_{
                sample_number}_a DISABLE KEYS"))

    con.execute(text(f"""
    INSERT INTO {table_prefix}submissions_sample_{sample_number}_a
    WITH random_thread_ids AS (
        SELECT id
        FROM {table_prefix}submissions_a
        WHERE author!="[deleted]" AND author!="AutoModerator" AND author!="[removed]" and selftext!="[removed]"
        ORDER BY RAND()
        LIMIT {submissions}
    )
    SELECT s.*
    FROM {table_prefix}submissions_a s
    INNER JOIN random_thread_ids USING (id)
    """))

    con.execute(text(f"""
    INSERT INTO {table_prefix}comments_sample_{sample_number}_a
    SELECT c.*
    FROM {table_prefix}comments_a c
    WHERE link_id IN (SELECT id FROM {table_prefix}submissions_sample_{sample_number}_a)
    """))

    con.execute(
        text(f"ALTER TABLE {table_prefix}submissions_sample_{sample_number}_a ENABLE KEYS"))
    con.execute(text(f"ALTER TABLE {table_prefix}comments_sample_{
                sample_number}_a ENABLE KEYS"))

    # Write local tsv copies
    (pd
        .read_sql_table(f"{table_prefix}submissions_sample_{sample_number}_a", con)
        .sort_values("created_utc")
        .to_csv(here(f"data/work/samples/{table_prefix}submissions_sample_{sample_number}.tsv"), index=False, sep="\t"))

    (pd
        .read_sql_table(f"{table_prefix}comments_sample_{sample_number}_a", con)
        .sort_values(["link_id", "created_utc"])
        .to_csv(here(f"data/work/samples/{table_prefix}comments_sample_{sample_number}.tsv"), index=False, sep="\t"))


if __name__ == '__main__':
    sample_threads()
