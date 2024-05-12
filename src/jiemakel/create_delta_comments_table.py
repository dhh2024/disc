# This code creates the cmw_delta_comments table in the database.
# %%

from hereutil import here, add_to_sys_path
from sqlalchemy import text
add_to_sys_path(here())
from src.common_basis import get_db_connection  # noqa

_, con = get_db_connection()

con.execute(text("DROP TABLE IF EXISTS cmw_delta_comments_a"))
con.execute(text("CREATE TABLE cmw_delta_comments_a LIKE cmw_comments_a"))
con.execute(text("ALTER TABLE cmw_delta_comments_a DISABLE KEYS"))
con.execute(text("""
INSERT INTO cmw_delta_comments_a
WITH delta_comments AS (
    SELECT DISTINCT parent_comment_id AS id
    FROM cmw_comments_a
    WHERE author = "DeltaBot" AND MATCH (body) AGAINST ("Confirmed" IN BOOLEAN MODE)
)
SELECT c.*
FROM delta_comments dc
INNER JOIN cmw_comments_a c USING (id)
"""))
con.execute(text("ALTER TABLE cmw_delta_comments_a ENABLE KEYS"))

# %%
