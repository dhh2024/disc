# %%
from pprint import pprint
import pandas as pd
from hereutil import here, add_to_sys_path
add_to_sys_path(here())
from src.common_basis import *  # noqa

# %%
a3s = get_s3fs()
a3s.ls("dhh24/disc/parquet")
# %%

# Shows reading parquet data from S3
cmw_submissions_sample_a = pd.read_parquet(
    path="dhh24/disc/parquet/cmw_submissions_sample_a.parquet", filesystem=a3s, engine="pyarrow")
cmw_comments_sample_a = pd.read_parquet(
    path="dhh24/disc/parquet/cmw_comments_sample_a.parquet", filesystem=a3s, engine="pyarrow")
cmw_comments_sample_a.head()
# %%

# get a submission and all its comments as a tree of Python object
_, con = get_db_connection()
pprint(get_submission(con, "cmw_submissions_sample_a",
                      "cmw_comments_sample_a", "t3_10g3juo"))
# %%
