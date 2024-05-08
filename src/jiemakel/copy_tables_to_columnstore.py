#!/usr/bin/env python3
import logging
from contextlib import closing
import click
import mariadb
from mariadb.cursors import Cursor
from hereutil import here, add_to_sys_path
add_to_sys_path(here())
from src.common_basis import get_params  # noqa

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def copy_table(cur: Cursor, tbl: str):
    logging.info(f"Copying {tbl} table to ColumnStore.")
    cur.execute(f"DROP TABLE IF EXISTS {tbl}_b")
    cur.execute(
        f"CREATE TABLE {tbl}_b ENGINE=ColumnStore DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci SELECT * FROM {tbl}_a WHERE 0")
    cur.execute(f"""
                INSERT INTO {tbl}_b SELECT * FROM {tbl}_a
                WHERE
                    id != "t1_dbkufkd" AND
                    id != "t1_evflaqh" AND
                    id != "t1_cxbbpkf"
                """)
    cur.execute(f"DROP TABLE IF EXISTS {tbl}_c")
    cur.execute(f"RENAME TABLE {tbl}_b TO {tbl}_c")


@click.option('-tp', '--table_prefix', required=True, help="table prefix")
@click.command
def copy_to_columnstore(table_prefix: str):
    """Copy tables from Aria to ColumnStore"""
    p = get_params()['db']
    with closing(mariadb.connect(user=p['db_user'],
                                 password=p['db_pass'],
                                 host=p['db_host'],
                                 port=3306,
                                 database=p['db_name'],
                                 autocommit=True)) as conn, closing(conn.cursor()) as cur:
        cur: Cursor
        copy_table(cur, f"{table_prefix}submissions")
        copy_table(cur, f"{table_prefix}comments")
        logging.info("Done.")


if __name__ == '__main__':
    copy_to_columnstore()
