

# %%
from hereutil import here, add_to_sys_path
add_to_sys_path(here())
from src.common_basis import *
import pandas as pd



dd_sample = pd.read_parquet('dhh24/disc/parquet/data_driven_civil_discourse_subset_a.parquet', filesystem=get_s3fs(),engine="pyarrow")
dd_sample = dd_sample[~dd_sample['body'].str.contains(r"\[removed\]|\[deleted\]|your comment has been removed", na=True)]
dd_sample = dd_sample.sample(4000).copy()
dd_sample.to_csv(here('data/work/samples/data_driven_4000_sample.tsv'), sep='\t')

random_sample = pd.read_parquet('dhh24/disc/parquet/random_sample_comments_a.parquet', filesystem=get_s3fs(),engine="pyarrow")
random_sample = random_sample[~random_sample['body'].str.contains(r"\[removed\]|\[deleted\]|your comment has been removed", na=True)]
random_sample = random_sample.sample(4000).copy()
random_sample.to_csv(here('data/work/samples/random_4000_sample.tsv'), sep='\t')
