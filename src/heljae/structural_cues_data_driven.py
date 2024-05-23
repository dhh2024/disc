#%%
from hereutil import here, add_to_sys_path
import pandas as pd
import re

add_to_sys_path(here())
from src.common_basis import *

#%%
# get the samples
dd_sample = pd.read_csv(here("data/work/samples/data_driven_4000_sample.tsv"), sep="\t")
random_sample = pd.read_csv(here("data/work/samples/random_4000_sample.tsv"), sep="\t")

#%%
# message length
dd_sample_lens = dd_sample['body'].map(lambda x: len(str(x).split())) 
dd_sample['comment_length'] = dd_sample_lens

random_sample_lens = random_sample['body'].map(lambda x: len(str(x).split())) 
random_sample['comment_length'] = random_sample_lens


# comments with quotes
p = r"\s*>.[\sA-Za-z\d\"\']+|\s*\&gt;"

# find
dd_quotes = dd_sample['body'].apply(lambda x: len(re.findall(p,x)))
# apply to data
dd_sample['n_of_quotes'] = dd_quotes

random_quotes = random_sample['body'].apply(lambda x: len(re.findall(p,x)))
# apply to data
random_sample['n_of_quotes'] = random_quotes

# contains links

#%%
dd_links = dd_sample['body'].str.contains("(http", regex=False)
dd_sample['contains_links'] = dd_links

random_links = random_sample['body'].str.contains("(http", regex=False)
random_sample['contains_links'] = random_links

dd_sample.to_csv(here("data/work/samples/data_driven_4000_sample.tsv"), sep='\t')
random_sample.to_csv(here("data/work/samples/random_4000_sample.tsv"), sep='\t')

#%%
if __name__ == "__main__":
    #draw_histogram(
    #dd_sample_lens, random_sample_lens,
    #'Civil sample',
    #'Random sample',
    #'Comment lengths',
    #'n of words',
    #list(range(0,1875,250)),
    #'skyblue')
    pass
# %%
