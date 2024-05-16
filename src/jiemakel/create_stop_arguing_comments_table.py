# This code creates the cmw_delta_comments table in the database.
# %%

from hereutil import here, add_to_sys_path
from sqlalchemy import text
add_to_sys_path(here())
from src.common_basis import get_db_connection  # noqa

_, con = get_db_connection()

con.execute(text("DROP TABLE IF EXISTS stop_arguing_stop_arguing_comments_a"))
con.execute(
    text("CREATE TABLE stop_arguing_stop_arguing_comments_a LIKE stop_arguing_comments_a"))
con.execute(text("ALTER TABLE stop_arguing_stop_arguing_comments_a DISABLE KEYS"))
con.execute(text("""
INSERT INTO stop_arguing_stop_arguing_comments_a
SELECT * 
FROM (
  SELECT *
  FROM stop_arguing_comments_a
  WHERE MATCH (body) AGAINST ('"stop arguing"' IN BOOLEAN MODE)
) as t
WHERE t.body REGEXP '(?m)^[^&][^g][^t][^;].*stop arguing|^&?g?t?stop arguing'
"""))
con.execute(text("ALTER TABLE stop_arguing_stop_arguing_comments_a ENABLE KEYS"))

# %%
