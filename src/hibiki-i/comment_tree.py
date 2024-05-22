from hereutil import here, add_to_sys_path
add_to_sys_path(here())

from src.common_basis import *

from sqlalchemy import text
import pandas as pd
eng, con = get_db_connection()

from hereutil import here, add_to_sys_path
add_to_sys_path(here())
from sqlalchemy import text
from src.common_basis import get_db_connection  # noqa

eng, con = get_db_connection()
con.execute(text("DROP TABLE IF EXISTS cmw_threads_c"))
con.execute(text("CREATE TABLE cmw_threads_c (thread_id BIGINT PRIMARY KEY, thread_size INT, delta INT)"))
con.execute(text("ALTER TABLE cmw_threads_c DISABLE KEYS"))

# Define the SQL query with placeholders for parameters
# delete 'YEAR(created_utc) = 2019' to get full data

query = text("""
INSERT INTO cmw_threads_c
WITH RECURSIVE ThreadHierarchy AS (
    SELECT id, parent_comment_id, 1 AS thread_size,
    CASE
        WHEN author = :author_param AND MATCH (body) AGAINST (:body_param IN BOOLEAN MODE) THEN 1
        ELSE 0
    END AS delta
    FROM cmw_comments_a
    WHERE parent_comment_id IS NULL AND YEAR(created_utc) = 2019
             
    UNION ALL
             
    SELECT c.id, c.parent_comment_id, th.thread_size + 1,
    CASE
        WHEN c.author = :author_param AND MATCH (c.body) AGAINST (:body_param IN BOOLEAN MODE) THEN th.delta + 1
        ELSE th.delta
    END
    FROM cmw_comments_a c
    INNER JOIN ThreadHierarchy th ON c.parent_comment_id = th.id
)
SELECT id AS thread_id, MAX(thread_size) AS thread_size, SUM(delta) AS delta
FROM ThreadHierarchy
GROUP BY id
""")

# Parameters to be passed into the query
params = {"author_param": "DeltaBot", "body_param": "Confirmed"}

# Execute the query with parameters
con.execute(query, params)

con.execute(text("ALTER TABLE cmw_threads_c ENABLE KEYS"))


# plot the probability that a comment tree ends in a delta 
import pandas as pd
import matplotlib.pyplot as plt

d = pd.read_sql(text("""
SELECT *
FROM cmw_threads_c
"""), con)

dat = d.groupby('thread_size').apply(lambda x: x['delta'].mean())
dat = pd.DataFrame(dat).reset_index()
dat.columns = ['thread_size', 'delta_rate']
plt.scatter(dat['thread_size'], dat['delta_rate'], alpha=0.5)
plt.xlabel('comment tree depth')
plt.title('probability that a comment tree ends in a delta')
plt.show()