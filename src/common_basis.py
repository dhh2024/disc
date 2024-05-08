from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any
import getpass
import sqlalchemy
import yaml
import s3fs
from functools import cache
from hereutil import here
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import Engine, Connection
import findspark
import os
from pyspark.sql import SparkSession


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


@cache
def get_db_connection() -> tuple[Engine, Connection]:
    """Connect to the database, returning both the SQLAlchemy engine and connection."""
    db_params = get_params()['db']
    eng = sqlalchemy.create_engine(
        "mariadb+pymysql://" + db_params['db_user'] + ":" + db_params['db_pass'] + "@" + db_params['db_host'] + "/" +
        db_params['db_name'] +
        "?charset=utf8mb4&autocommit&local_infile",
        future=True
    )
    con = eng.connect()
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


__all__ = ["get_db_connection", "get_params", "set_session_storage_engine", "get_s3fs",
           "get_spark", "spark_jdbc_opts", "Submission", "Comment", "get_submission"]
