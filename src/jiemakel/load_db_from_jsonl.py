#!/usr/bin/env python3
import dataclasses
import datetime
import itertools
import os
from dataclasses import dataclass, astuple, field
from functools import reduce
from typing import Iterable, TextIO, Any, Callable, TypeVar

import click
from contextlib import closing
from more_itertools import chunked
from json import JSONDecodeError
import tqdm
import json
import logging
import mariadb
from mariadb.connections import Connection
from mariadb.cursors import Cursor
import pyzstd
import glob

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class RecoveringCursor:
    def __init__(self, **config):
        self.config = config
        self.conn = mariadb.connect(**self.config)
        self.cur = self.conn.cursor()

    def executemany(self, stmt, data):
        if len(data) > 0:
            chunk_size = 1000
            for db in chunked(data, chunk_size):
                while True:
                    try:
                        for db2 in chunked(db, chunk_size):
                            self.cur.executemany(stmt, db2)
                            if self.cur.warnings > 0:
                                self.cur.execute("SHOW WARNINGS")
                                warnings = list(
                                    filter(lambda warning: warning[1] != 1062, self.cur.fetchall()))
                                if len(warnings) > 0:
                                    logging.warning(warnings)
                        break
                    except mariadb.InterfaceError:
                        logging.error(db)
                        self.cur.close()
                        self.conn.close()
                        self.conn = mariadb.connect(**self.config)
                        self.cur = self.conn.cursor()
                        chunk_size = max(chunk_size // 2, 1)
                    except (mariadb.DataError, mariadb.ProgrammingError):
                        #                        logging.exception(data)
                        logging.exception(data[685])
                        raise

    def fetchall(self):
        while True:
            try:
                return self.cur.fetchall()
            except mariadb.InterfaceError:
                logging.error("InterfaceError")
                self.cur.close()
                self.conn.close()
                self.conn = mariadb.connect(**self.config)
                self.cur = self.conn.cursor()

    def close(self):
        self.cur.close()
        self.conn.close()


@dataclass
class Submission:
    subreddit_name: str
    id: str
    permalink: str
    created_utc: datetime.datetime
    author: str
    title: str
    url: str | None
    selftext: str | None
    score: int
    num_comments: int
    upvote_ratio: float | None

    @classmethod
    def map_submission(cls, submission: dict):
        return cls(
            subreddit_name=submission['subreddit'],
            id="t3_" + submission['id'],
            permalink=f"https://www.reddit.com/{submission['permalink']}",
            created_utc=datetime.datetime.fromtimestamp(
                int(submission['created_utc']), datetime.UTC),
            author=submission['author'],
            title=submission['title'],
            url=submission['url'] if submission['url'] != '' else None,
            selftext=submission['selftext'] if submission['selftext'] != '' else None,
            score=submission['score'],
            num_comments=submission['num_comments'],
            upvote_ratio=float(
                submission['upvote_ratio']) if 'upvote_ratio' in submission else None
        )

    @staticmethod
    def prepare_submission_tables(conn: Connection, table_prefix: str):
        with closing(conn.cursor()) as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_prefix}submissions_i;")
            cur.execute(f"""
                CREATE TABLE {table_prefix}submissions_i (
                    subreddit_name VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    id CHAR(10) CHARACTER SET utf8mb4 NOT NULL PRIMARY KEY,
                    permalink VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    created_utc TIMESTAMP NOT NULL,
                    author VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    title VARCHAR(510) CHARACTER SET utf8mb4 NOT NULL,
                    url VARCHAR(510) CHARACTER SET utf8mb4,
                    selftext TEXT CHARACTER SET utf8mb4,
                    score INTEGER NOT NULL,
                    num_comments INTEGER UNSIGNED NOT NULL,
                    upvote_ratio FLOAT
                ) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0""")

    def as_tuple(self):
        return tuple(getattr(self,
                             field.name) for field in
                     dataclasses.fields(self))

    @staticmethod
    def insert_stmt(table_prefix: str):
        return f"INSERT IGNORE INTO {table_prefix}submissions_i VALUES ({'%s,' * 10}%s)"


def base36encode(integer: int) -> str:
    chars = '0123456789abcdefghijklmnopqrstuvwxyz'
    sign = '-' if integer < 0 else ''
    integer = abs(integer)
    result = ''
    while integer > 0:
        integer, remainder = divmod(integer, 36)
        result = chars[remainder] + result
    return sign + result


@dataclass
class Comment:
    subreddit_name: str
    id: str
    permalink: str
    link_id: str
    parent_id: str
    created_utc: datetime.datetime
    author: str
    body: str | None
    score: int
    controversiality: bool

    @classmethod
    def map_comment(cls, comment: dict):
        return cls(
            subreddit_name=comment['subreddit'],
            id="t1_" + comment['id'],
            permalink=f"https://www.reddit.com/r/{comment['subreddit']}/comments/{
                comment['link_id'][3:]}/_/{comment['id']}/",
            link_id=comment['link_id'],
            parent_id=('t3_'+base36encode(comment['parent_id'])) if type(
                comment['parent_id']) == int else comment['parent_id'],
            created_utc=datetime.datetime.fromtimestamp(
                int(comment['created_utc']), datetime.UTC),
            author=comment['author'],
            body=comment['body'] if comment['body'] != '' else None,
            score=comment['score'],
            controversiality=comment['controversiality'] == 1
        )

    @staticmethod
    def prepare_comments_table(conn: Connection, table_prefix: str):
        with closing(conn.cursor()) as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_prefix}comments_i;")
            cur.execute(f"""
                CREATE TABLE {table_prefix}comments_i (
                    subreddit_name VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    id CHAR(10) CHARACTER SET utf8mb4 NOT NULL PRIMARY KEY,
                    permalink VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    link_id CHAR(10) CHARACTER SET utf8mb4 NOT NULL,
                    parent_id CHAR(10) CHARACTER SET utf8mb4 NOT NULL,
                    created_utc TIMESTAMP NOT NULL,
                    author VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    body TEXT CHARACTER SET utf8mb4,
                    score INTEGER NOT NULL,
                    controversiality BOOLEAN
                ) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0""")

    def as_tuple(self):
        return tuple(getattr(self,
                             field.name) for field in
                     dataclasses.fields(self))

    @staticmethod
    def insert_stmt(table_prefix: str):
        return f"INSERT IGNORE INTO {table_prefix}comments_i VALUES ({'%s,' * 9}%s)"


def map_recover(f: Callable[[Any], Any], it: Iterable[Any]) -> Iterable[Any]:
    for t in it:
        try:
            yield f(t)
        except KeyError:
            logging.exception(f"KeyError parsing {t}. Skipping.")


def yield_submissions(submissions_file: TextIO) -> Iterable[Submission]:
    json_obj = submissions_file.readline()
    line_number = 1
    while json_obj:
        try:
            r = json.loads(json_obj)
            try:
                yield Submission.map_submission(r)
            except KeyError:
                logging.exception(f"KeyError parsing {r}. Skipping.")
        except JSONDecodeError:
            logging.exception(
                f"Exception parsing line number {line_number}. Skipping.")
        json_obj = submissions_file.readline()
        line_number += 1


def yield_comments(comments_file: TextIO) -> Iterable[Comment]:
    json_obj = comments_file.readline()
    line_number = 1
    while json_obj:
        try:
            r = json.loads(json_obj)
            try:
                yield Comment.map_comment(r)
            except KeyError:
                logging.exception(f"KeyError parsing {r}. Skipping.")
        except JSONDecodeError:
            logging.exception(
                f"Exception parsing line number {line_number}. Skipping.")
        json_obj = comments_file.readline()
        line_number += 1


@click.option('-u', '--username', required=True, help="database username")
@click.option('-p', '--password', required=True, help="database password")
@click.option('-h', '--host', required=True, help="database hostname")
@click.option('-d', '--database', required=True, help="database name")
@click.option('-s', '--submissions', required=True, multiple=True, help="input jsonl.zst files containing submissions")
@click.option('-c', '--comments', required=True, multiple=True, help="input jsonl.zst files containing comments")
@click.option('-tp', '--table-prefix', required=True, help="table prefix")
@click.command
def load_db(username: str, password: str, host: str, database: str, submissions: list[str], comments: list[str], table_prefix: str):
    """Load tweets into the database"""
    with closing(mariadb.connect(user=username,
                                 password=password,
                                 host=host,
                                 port=3306,
                                 autocommit=True)) as conn, closing(conn.cursor()) as cur:
        cur: Cursor  # type: ignore
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {database};")

    with closing(mariadb.connect(user=username,
                                 password=password,
                                 host=host,
                                 port=3306,
                                 database=database,
                                 autocommit=True)) as conn:
        conn: Connection
        logging.info("Preparing submissions tables.")
        Submission.prepare_submission_tables(conn, table_prefix)
        logging.info("Preparing comments table.")
        Comment.prepare_comments_table(conn, table_prefix)
        logging.info("Loading data.")
        submissions = list(itertools.chain.from_iterable([glob.glob(
            submission, recursive=True) for submission in submissions]))
        comments = list(itertools.chain.from_iterable(
            [glob.glob(comment, recursive=True) for comment in comments]))
        with closing(RecoveringCursor(user=username,
                                      password=password,
                                      host=host,
                                      port=3306,
                                      database=database,
                                      autocommit=True)) as cur:
            cur: RecoveringCursor  # type: ignore
            tsize = reduce(lambda tsize, submssion_file_name: tsize +
                           os.path.getsize(submssion_file_name), submissions, 0)
            processed_files_tsize = 0
            pbar = tqdm.tqdm(total=tsize, unit='b',
                             unit_scale=True, unit_divisor=1024, dynamic_ncols=True)
            for submissions_file_name in submissions:
                pbar.set_description(submissions_file_name)
                # logging.info(f"Starting to process file {submissions_file_name}.")
                with open(submissions_file_name, 'rb') as zf, pyzstd.open(zf, 'rt') as submissions_file:
                    for comms in chunked(yield_submissions(submissions_file), 1000):
                        cur.executemany(Submission.insert_stmt(table_prefix), [
                            sub.as_tuple() for sub in comms])
                        pbar.n = processed_files_tsize + zf.tell()
                        pbar.update(0)
                processed_files_tsize += os.path.getsize(submissions_file_name)
            tsize = reduce(lambda tsize, comments_file_name: tsize +
                           os.path.getsize(comments_file_name), comments, 0)
            processed_files_tsize = 0
            pbar = tqdm.tqdm(total=tsize, unit='b',
                             unit_scale=True, unit_divisor=1024, dynamic_ncols=True)
            for comments_file_name in comments:
                pbar.set_description(comments_file_name)
                # logging.info(
                #    f"Starting to process file {comments_file_name}.")
                with open(comments_file_name, 'rb') as zf, pyzstd.open(zf, 'rt') as comments_file:
                    for comms in chunked(yield_comments(comments_file), 1000):
                        cur.executemany(Comment.insert_stmt(table_prefix), [
                            com.as_tuple() for com in comms])
                        pbar.n = processed_files_tsize + zf.tell()
                        pbar.update(0)
                processed_files_tsize += os.path.getsize(comments_file_name)

        with closing(conn.cursor()) as cur:
            cur: Cursor  # type: ignore
            logging.info('Insert complete. Adding indices.')
            cur.execute(
                f"""
                ALTER TABLE {table_prefix}submissions_i
                ADD FULLTEXT(title),
                ADD FULLTEXT(selftext),
                ADD UNIQUE INDEX (created_utc, id),
                ADD UNIQUE INDEX (author, id),
                ADD UNIQUE INDEX (score, id)
                """)
            cur.execute(
                f"""
                ALTER TABLE {table_prefix}comments_i
                ADD FULLTEXT(body),
                ADD UNIQUE INDEX (created_utc, id),
                ADD UNIQUE INDEX (parent_id, id),
                ADD UNIQUE INDEX (link_id, id),
                ADD UNIQUE INDEX (author, id),
                ADD UNIQUE INDEX (score, id)
                """)
            logging.info('Done adding indices. Renaming tables.')
            cur.execute(f"DROP TABLE IF EXISTS {table_prefix}submissions_a")
            cur.execute(
                f"RENAME TABLE {table_prefix}submissions_i TO {table_prefix}submissions_a")
            cur.execute(f"DROP TABLE IF EXISTS {table_prefix}comments_a")
            cur.execute(
                f"RENAME TABLE {table_prefix}comments_i TO {table_prefix}comments_a")
            logging.info('Done.')


if __name__ == '__main__':
    load_db()
