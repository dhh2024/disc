#!/usr/bin/env python3
import glob
import pyzstd
from mariadb.cursors import Cursor
from mariadb.connections import Connection
import mariadb
import logging
import json
import tqdm
from json import JSONDecodeError
from more_itertools import chunked
from contextlib import closing
import click
from typing import Iterable, TextIO, Any, Callable, TypeVar
from functools import reduce
from dataclasses import dataclass, astuple, field
import os
import itertools
import datetime
import dataclasses
from hereutil import here, add_to_sys_path
add_to_sys_path(here())
from src.common_basis import RecoveringCursor, base36decode, get_params, LRUCache  # noqa


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


@dataclass
class Submission:
    subreddit_id: int
    subreddit: str
    id: int
    permalink: str
    created_utc: datetime.datetime
    author_id: int | None
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
            subreddit_id=base36decode(submission['subreddit_id'][3:]),
            subreddit=submission['subreddit'],
            id=base36decode(submission['id']),
            permalink=f"https://www.reddit.com/{submission['permalink']}",
            created_utc=datetime.datetime.fromtimestamp(
                int(submission['created_utc']), datetime.UTC),
            author_id=base36decode(
                submission['author_fullname'][3:]) if 'author_fullname' in submission and submission['author_fullname'] is not None else None,
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
            cur.execute(f"DROP TABLE IF EXISTS {table_prefix}submissions_i")
            cur.execute(f"""
                CREATE TABLE {table_prefix}submissions_i (
                    subreddit_id BIGINT UNSIGNED NOT NULL,
                    subreddit VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                    permalink VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    created_utc TIMESTAMP NOT NULL,
                    author_id BIGINT UNSIGNED,
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
        return f"INSERT IGNORE INTO {table_prefix}submissions_i VALUES ({'%s,' * 12}%s)"


@dataclass
class Comment:
    subreddit_id: int
    subreddit: str
    id: int
    permalink: str
    link_id: int
    parent_comment_id: int | None
    created_utc: datetime.datetime
    author_id: int | None
    author: str
    body: str | None
    score: int
    controversiality: bool

    @classmethod
    def map_comment(cls, comment: dict):
        parent_comment_id = comment['parent_id']
        if type(parent_comment_id) == str:
            if parent_comment_id[:3] == 't3_':
                parent_comment_id = None
            else:
                parent_comment_id = base36decode(parent_comment_id[3:])
        return cls(
            subreddit_id=base36decode(comment['subreddit_id'][3:]),
            subreddit=comment['subreddit'],
            id=base36decode(comment['id']),
            permalink=f"https://www.reddit.com/r/{comment['subreddit']}/comments/{
                comment['link_id'][3:]}/_/{comment['id']}/",
            link_id=base36decode(comment['link_id'][3:]),
            parent_comment_id=parent_comment_id,
            created_utc=datetime.datetime.fromtimestamp(
                int(comment['created_utc']), datetime.UTC),
            author_id=base36decode(
                comment['author_fullname'][3:]) if 'author_fullname' in comment and comment['author_fullname'] is not None else None,
            author=comment['author'],
            body=comment['body'] if comment['body'] != '' else None,
            score=comment['score'],
            controversiality=comment['controversiality'] == 1
        )

    @ staticmethod
    def prepare_comments_table(conn: Connection, table_prefix: str):
        with closing(conn.cursor()) as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_prefix}comments_i;")
            cur.execute(f"""
                CREATE TABLE {table_prefix}comments_i (
                    subreddit_id BIGINT UNSIGNED NOT NULL,
                    subreddit VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                    permalink VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    link_id BIGINT UNSIGNED NOT NULL,
                    parent_comment_id BIGINT UNSIGNED,
                    created_utc TIMESTAMP NOT NULL,
                    author_id BIGINT UNSIGNED,
                    author VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
                    body TEXT CHARACTER SET utf8mb4,
                    score INTEGER NOT NULL,
                    controversiality BOOLEAN
                ) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0""")

    def as_tuple(self):
        return tuple(getattr(self,
                             field.name) for field in
                     dataclasses.fields(self))

    @ staticmethod
    def insert_stmt(table_prefix: str):
        return f"INSERT IGNORE INTO {table_prefix}comments_i VALUES ({'%s,' * 11}%s)"


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
            except (KeyError, TypeError) as e:
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
            except (KeyError, TypeError) as e:
                logging.exception(f"KeyError parsing {r}. Skipping.")
        except JSONDecodeError:
            logging.exception(
                f"Exception parsing line number {line_number}. Skipping.")
        json_obj = comments_file.readline()
        line_number += 1


@ click.option('-s', '--submissions', required=True, multiple=True, help="input jsonl.zst files containing submissions")
@ click.option('-c', '--comments', required=True, multiple=True, help="input jsonl.zst files containing comments")
@ click.option('-tp', '--table-prefix', required=True, help="table prefix")
@ click.command
def load_db(submissions: list[str], comments: list[str], table_prefix: str):
    p = get_params()['db']
    with closing(mariadb.connect(user=p['db_user'],
                                 password=p['db_pass'],
                                 host=p['db_host'],
                                 port=3306,
                                 autocommit=True)) as conn, closing(conn.cursor()) as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {
                    p['db_name']} DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_general_ci")

    with closing(mariadb.connect(user=p['db_user'],
                                 password=p['db_pass'],
                                 host=p['db_host'],
                                 port=3306,
                                 database=p['db_name'],
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
        with closing(RecoveringCursor(user=p['db_user'],
                                      password=p['db_pass'],
                                      host=p['db_host'],
                                      port=3306,
                                      database=p['db_name'],
                                      autocommit=True)) as rcur:
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
                        rcur.executemany(Submission.insert_stmt(table_prefix), [
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
                        rcur.executemany(Comment.insert_stmt(table_prefix), [
                            com.as_tuple() for com in comms])
                        pbar.n = processed_files_tsize + zf.tell()
                        pbar.update(0)
                processed_files_tsize += os.path.getsize(comments_file_name)
        logging.info('Insert complete.')
        with closing(conn.cursor()) as cur:
            logging.info('Adding indices.')
            cur.execute(
                f"""
                ALTER TABLE {table_prefix}submissions_i
                ADD FULLTEXT(title),
                ADD FULLTEXT(selftext),
                ADD UNIQUE INDEX (created_utc, id),
                ADD UNIQUE INDEX (author, id),
                ADD UNIQUE INDEX (author_id, id),
                ADD UNIQUE INDEX (score, id)
                """)
            cur.execute(
                f"""
                ALTER TABLE {table_prefix}comments_i
                ADD FULLTEXT(body),
                ADD UNIQUE INDEX (created_utc, id),
                ADD UNIQUE INDEX (parent_comment_id, id),
                ADD UNIQUE INDEX (link_id, id),
                ADD UNIQUE INDEX (author, id),
                ADD UNIQUE INDEX (author_id, id),
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
