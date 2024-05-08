# This code creates smaller sample tables from the original tables in the database.

from hereutil import here, add_to_sys_path
import pandas as pd
from sqlalchemy import text
from contextlib import closing

# %%
table_prefix = "eli5_"
submissions_to_sample = 1000
sample_number = 1
overwrite = True

# %%
add_to_sys_path(here())
from src.common_basis import get_db_connection  # noqa
eng, con = get_db_connection()
# %%
if overwrite:
    con.execute(text(f"DROP TABLE IF EXISTS {table_prefix}submissions_sample_{sample_number}_a"))
con.execute(text(
    f"CREATE TABLE {table_prefix}submissions_sample_{sample_number}_a LIKE {table_prefix}submissions_a"))
con.execute(
    text(f"ALTER TABLE {table_prefix}submissions_sample_{sample_number}_a DISABLE KEYS"))
if overwrite:
    con.execute(text(f"DROP TABLE IF EXISTS {table_prefix}comments_sample_{sample_number}_a"))
con.execute(text(
    f"CREATE TABLE {table_prefix}comments_sample_{sample_number}_a LIKE {table_prefix}comments_a"))
con.execute(text(f"ALTER TABLE {table_prefix}comments_sample_{sample_number}_a DISABLE KEYS"))

# %%
con.execute(text(f"""
INSERT INTO {table_prefix}submissions_sample_{sample_number}_a
WITH random_thread_ids AS (
    SELECT id 
    FROM {table_prefix}submissions_a 
    WHERE author!="[deleted]" AND author!="AutoModerator" AND author!="[removed]"
    ORDER BY RAND()
    LIMIT {submissions_to_sample}
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

# %%
con.execute(
    text(f"ALTER TABLE {table_prefix}submissions_sample_{sample_number}_a ENABLE KEYS"))
con.execute(text(f"ALTER TABLE {table_prefix}comments_sample_{sample_number}_a ENABLE KEYS"))

# %%

(pd
    .read_sql_table(f"{table_prefix}submissions_sample_{sample_number}_a", con)
    .to_csv(here(f"data/work/samples/{table_prefix}submissions_sample_{sample_number}.tsv"), index=False, sep="\t"))

(pd
    .read_sql_table(f"{table_prefix}comments_sample_{sample_number}_a", con)
    .to_csv(here(f"data/work/samples/{table_prefix}comments_sample_{sample_number}.tsv"), index=False, sep="\t"))
