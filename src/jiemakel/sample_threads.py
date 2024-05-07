# This code creates smaller sample tables from the original tables in the database.

from hereutil import here, add_to_sys_path
import pandas as pd
from sqlalchemy import text
from contextlib import closing

# %%
table_prefix = "cmw_"
submissions_to_sample = 1000

# %%
add_to_sys_path(here())
from src.common_basis import get_db_connection  # noqa
eng, con = get_db_connection()
# %%
threads = pd.read_sql_query(f"""
WITH random_thread_ids AS (
    SELECT id 
    FROM {table_prefix}submissions_a 
    WHERE author!="[deleted]" AND author!="AutoModerator" AND author!="[removed]"
    ORDER BY RAND()
    LIMIT {submissions_to_sample}
)
SELECT * 
FROM {table_prefix}submissions_a 
INNER JOIN random_thread_ids USING (id)
    
""", con)
# %%
messages = pd.read_sql_query(f"""
    SELECT * 
    FROM {table_prefix}comments_a
    WHERE link_id IN ("{'", "'.join(threads['id'])}")
""", con)
# %%
con.execute(text(f"DROP TABLE IF EXISTS {table_prefix}submissions_sample_a"))
con.execute(text(
    f"CREATE TABLE {table_prefix}submissions_sample_a LIKE {table_prefix}submissions_a"))
con.execute(
    text(f"ALTER TABLE {table_prefix}submissions_sample_a DISABLE KEYS"))
con.execute(text(f"DROP TABLE IF EXISTS {table_prefix}comments_sample_a"))
con.execute(text(
    f"CREATE TABLE {table_prefix}comments_sample_a LIKE {table_prefix}comments_a"))
con.execute(text(f"ALTER TABLE {table_prefix}comments_sample_a DISABLE KEYS"))

# %%
threads.to_sql(f"{table_prefix}submissions_sample_a", con,
               index=False, if_exists='append')
messages.to_sql(f"{table_prefix}comments_sample_a",
                con, index=False, if_exists='append')

# %%
con.execute(text(f"""
INSERT INTO {table_prefix}submissions_sample_a
WITH random_thread_ids AS (
    SELECT id 
    FROM {table_prefix}submissions_a 
    WHERE author!="[deleted]" AND author!="AutoModerator" AND author!="[removed]"
    ORDER BY RAND()
    LIMIT 1000
)
SELECT s.* 
FROM {table_prefix}submissions_a s
INNER JOIN random_thread_ids USING (id)                 
"""))

con.execute(text(f"""
INSERT INTO {table_prefix}comments_sample_a
SELECT c.* 
FROM {table_prefix}comments_a c
WHERE link_id IN (SELECT id FROM {table_prefix}submissions_sample_a)
"""))

# %%
con.execute(
    text(f"ALTER TABLE {table_prefix}submissions_sample_a ENABLE KEYS"))
con.execute(text(f"ALTER TABLE {table_prefix}comments_sample_a ENABLE KEYS"))
