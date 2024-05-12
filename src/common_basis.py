from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from typing import Any, Generic, Sequence, TypeVar
import sqlalchemy
import yaml
import s3fs
from functools import cache
from hereutil import here
from sqlalchemy import text
from sqlalchemy.future import Engine, Connection
import mariadb
from mariadb.cursors import Cursor as MDBCursor
from mariadb.connections import Connection as MDBConnection

import findspark
import os
from pyspark.sql import SparkSession
import logging
from more_itertools import chunked

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class RecoveringCursor:
    def __init__(self, **config):
        self.config = config
        self.con = mariadb.connect(**self.config)
        self.cur = self.con.cursor()

    def execute(self, statement: str, data: Sequence = (), buffered=None) -> 'RecoveringCursor':
        self.cur.execute(statement, data, buffered)
        return self

    def executemany(self, stmt: str, data: Sequence) -> 'RecoveringCursor':
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
                        self.con.close()
                        self.con = mariadb.connect(**self.config)
                        self.cur = self.con.cursor()
                        chunk_size = max(chunk_size // 2, 1)
                    except (mariadb.DataError, mariadb.ProgrammingError):
                        logging.exception(data)
                        raise
        return self

    def fetchall(self) -> Sequence[tuple]:
        while True:
            try:
                return self.cur.fetchall()
            except mariadb.InterfaceError:
                logging.error("InterfaceError")
                self.cur.close()
                self.con.close()
                self.con = mariadb.connect(**self.config)
                self.cur = self.con.cursor()

    def fetchmany(self, size: int = 0) -> Sequence[tuple]:
        while True:
            try:
                return self.cur.fetchmany(size)
            except mariadb.InterfaceError:
                logging.error("InterfaceError")
                self.cur.close()
                self.con.close()
                self.con = mariadb.connect(**self.config)
                self.cur = self.con.cursor()

    def fetchone(self) -> tuple:
        while True:
            try:
                return self.cur.fetchone()
            except mariadb.InterfaceError:
                logging.error("InterfaceError")
                self.cur.close()
                self.con.close()
                self.con = mariadb.connect(**self.config)
                self.cur = self.con.cursor()

    def __iter__(self):
        return iter(self.fetchone, None)

    def close(self):
        self.cur.close()
        self.con.close()


def base36encode(integer: int) -> str:
    chars = '0123456789abcdefghijklmnopqrstuvwxyz'
    sign = '-' if integer < 0 else ''
    integer = abs(integer)
    result = ''
    while integer > 0:
        integer, remainder = divmod(integer, 36)
        result = chars[remainder] + result
    return sign + result


def base36decode(base36: str) -> int:
    return int(base36, 36)


@cache
def get_params() -> dict[str, dict[str, str]]:
    """Get database/S3 parameters from a file"""
    if here("params.yaml").exists():
        with here("params.yaml").open('r') as f:
            d = yaml.safe_load(f)
    else:
        d = dict()
    if here("secret.yaml").exists():
        with here("secret.yaml").open('r') as f:
            secret = yaml.safe_load(f)
        for key, value in secret.items():
            if key in d:
                d[key].update(value)
            else:
                d[key] = value
    return d


def get_recovering_cursor() -> RecoveringCursor:
    p = get_params()['db']
    return RecoveringCursor(user=p['db_user'],
                            password=p['db_pass'],
                            host=p['db_host'],
                            database=p['db_name'],
                            port=3306,
                            autocommit=True)


def get_db_connection() -> tuple[Engine, Connection]:
    """Connect to the database, returning both the SQLAlchemy engine and connection."""
    db_params = get_params()['db']
    eng = sqlalchemy.create_engine(
        "mariadb+pymysql://" + db_params['db_user'] + ":" + db_params['db_pass'] + "@" + db_params['db_host'] + "/" +
        db_params['db_name'] +
        "?charset=utf8mb4&autocommit&local_infile",
        future=True
    )
    con = eng.connect().execution_options(isolation_level="AUTOCOMMIT")
    return eng, con


def set_session_storage_engine(con: Connection, engine: str):
    """Set the MariaDB storage engine to the given value for the session so that new tables are created using it."""
    con.execute(text("SET SESSION storage_engine="+engine))


@cache
def get_s3fs() -> s3fs.S3FileSystem:
    """Configure and return an S3FileSystem with the necessary credentials."""
    params = get_params()['s3']
    d = dict()
    d.update(params)
    d['key'] = params['access_key_id']
    d['secret'] = params['secret_access_key']
    del d['access_key_id']
    del d['secret_access_key']
    return s3fs.S3FileSystem(**d, default_fill_cache=False, use_listings_cache=False)


def get_spark(app: str = "default") -> SparkSession:
    """Configure and return a SparkSession with the necessary JARs and S3 credentials."""
    findspark.init()
    spark = (SparkSession
             .builder
             .appName("app")
             .config("spark.jars.packages", 'org.mariadb.jdbc:mariadb-java-client:3.3.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.604')
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    s3_creds = get_params()['s3']
    # set up credentials for spark
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", s3_creds["access_key_id"])
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", s3_creds["secret_access_key"])
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.endpoint", s3_creds["endpoint_url"])
    return spark


def spark_jdbc_opts(con):
    """Configure JDBC options for Spark DataFrameReader and DataFrameWriter."""
    db_params = get_params()['db']
    return (con
            .format("jdbc")
            .option("driver", "org.mariadb.jdbc.Driver")
            .option("url", f"jdbc:mysql://{db_params['db_host']}:3306/{db_params['db_name']}?permitMysqlScheme")
            .option("user", db_params['db_user'])
            .option("password", db_params['db_pass'])
            .option("fetchsize", "100000")
            .option("batchsize", "100000"))


os.environ['AWS_ENDPOINT_URL'] = get_params()['s3']['endpoint_url']
os.environ['AWS_ACCESS_KEY_ID'] = get_params()['s3']['access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY'] = get_params()['s3']['secret_access_key']


@dataclass
class Submission:
    subreddit_name: str
    id: str
    permalink: str
    created_utc: int
    author: str
    title: str
    url: str | None
    selftext: str | None
    score: int
    num_comments: int
    upvote_ratio: float | None
    children: list['Comment'] = field(default_factory=list)


@dataclass
class Comment:
    subreddit_name: str
    id: str
    permalink: str
    link_id: str
    parent_id: str
    created_utc: int
    author: str
    body: str
    score: int
    controversiality: bool
    children: list['Comment'] = field(default_factory=list)


def get_submission(con: Connection, submissions_table: str, comments_table: str, id: str) -> Submission:
    comments = list(map(lambda r: Comment(**r._asdict()), con.execute(text(f"""
        SELECT *
        FROM {comments_table}
        WHERE link_id = :id
                     """), dict(id=id)).fetchall()))
    children = defaultdict(list)
    for c in comments:
        children[c.parent_id].append(c)
    for c in comments:
        c.children = children[c.id]
    submission = next(map(lambda r: Submission(**r._asdict()), con.execute(text(f"""
        SELECT *
        FROM {submissions_table}
        WHERE id = :id
                     """), dict(id=id)).fetchall()))
    submission.children = children[submission.id]
    return submission


K = TypeVar('K')
V = TypeVar('V')


class LRUCache(Generic[K, V]):
    def __init__(self, max_size=4):
        if max_size <= 0:
            raise ValueError

        self.max_size = max_size
        self._items: OrderedDict[K, V] = OrderedDict()

    def _move_latest(self, key) -> None:
        # Order is in descending priority, i.e. first element
        # is latest.
        self._items.move_to_end(key, last=False)

    def __getitem__(self, key: K, default: V | None = None) -> V | None:
        if key not in self._items:
            return default

        value = self._items[key]
        self._move_latest(key)
        return value

    def __setitem__(self, key: K, value: V):
        self._items[key] = value
        self._move_latest(key)

    def _trim_to_size(self):
        if len(self._items) > self.max_size:
            keys = list(self._items.keys())
            keys_to_evict = keys[-(len(self._items) - self.max_size):]
            for key in keys_to_evict:
                self._items.pop(key)


__all__ = ["get_db_connection", "get_recovering_cursor", "RecoveringCursor", "get_params", "set_session_storage_engine", "get_s3fs",
           "get_spark", "spark_jdbc_opts", "Submission", "Comment", "get_submission", "LRUCache"]
