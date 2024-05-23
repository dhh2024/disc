#%%
from hereutil import here, add_to_sys_path
add_to_sys_path(here())
from sqlalchemy import text
import pandas as pd
from src.common_basis import get_db_connection
import time


#%% running the below code will take around 20 minutes
start = time.time()
_, con = get_db_connection()
con.execute(text("DROP TABLE IF EXISTS cmw_threads_a"))
con.execute(text("CREATE TABLE cmw_threads_a (thread_id BIGINT PRIMARY KEY, thread_size INT, delta INT)"))

for year in range(2013, 2025):
    _, con = get_db_connection()
    con.execute(text("ALTER TABLE cmw_threads_a DISABLE KEYS"))

    query = text("""
    INSERT INTO cmw_threads_a            
    WITH RECURSIVE ThreadHierarchy AS (
        SELECT id, parent_comment_id, 1 AS thread_size,
        CASE
            WHEN author = :author_param AND MATCH (body) AGAINST (:body_param IN BOOLEAN MODE) THEN 1
            ELSE 0
        END AS delta
        FROM cmw_comments_a
        WHERE parent_comment_id IS NULL AND YEAR(created_utc) = :year_param
                
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

    params = {"author_param": "DeltaBot", "body_param": "Confirmed", "year_param": year}
    con.execute(query, params)
    con.execute(text("ALTER TABLE cmw_threads_a ENABLE KEYS"))
    t = time.time() - start
    print(f"{int(t // 60)}m{int(t % 60)}s: year {year} done")



#%% plot the probability that a comment tree ends in a delta 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

_, con = get_db_connection()

# d = pd.read_sql(text("""
# SELECT *
# FROM cmw_threads_a
# """), con)

# dat = d.groupby('thread_size').apply(lambda x: x['delta'].mean())
# dat = pd.DataFrame(dat).reset_index()
# dat.columns = ['thread_size', 'delta_rate']
# dat['delta'] = dat['delta_rate'] == 0

dat = pd.read_sql(text("""
                       SELECT thread_size, AVG(delta) AS delta_rate, 
                       CASE
                            WHEN AVG(delta) = 0 THEN :zero_param
                            ELSE :non_zero_param
                       END AS delta
                       FROM cmw_threads_a
                       GROUP BY thread_size
                       """), con, params={"non_zero_param": 'non-zero', "zero_param": 'zero'})

sns.scatterplot(data=dat, x='thread_size', y='delta_rate', hue='delta')
plt.xlabel('comment tree depth')
plt.title('probability that a comment tree ends in a delta')
plt.legend()
plt.show()
# %%
