# %%
from typing import Any
from sqlalchemy import text
from tqdm.auto import tqdm
from hereutil import here, add_to_sys_path
add_to_sys_path(here())  # noqa
from src.common_basis import *
import stanza

nlp = stanza.Pipeline(
    'en', processors='tokenize,mwt,pos,lemma,depparse,constituency,ner,sentiment')

# %%
cur = get_recovering_cursor()

# %%

cur.execute("""SELECT * FROM words_a WHERE word IN (?, ?)""", ('IN','in')).fetchall()
# %%
fs = get_s3fs()

# %%
