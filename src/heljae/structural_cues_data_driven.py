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

human_eli5_sample = pd.read_parquet(here("data/work/human-annotated-collected-eli5.parquet"))
human_random_sample = pd.read_parquet(here("data/work/human-annotated-collected-random.parquet"))
human_uo_sample = pd.read_parquet(here("data/work/human-annotated-collected-unpopular.parquet"))

# %%

#%%
# message length
#dd_sample_lens = dd_sample['body'].map(lambda x: len(str(x).split())) 
#dd_sample['comment_length'] = dd_sample_lens

#random_sample_lens = random_sample['body'].map(lambda x: len(str(x).split())) 
#random_sample['comment_length'] = random_sample_lens

h_eli5_lens = human_eli5_sample['body'].map(lambda x: len(str(x).split())) 
human_eli5_sample['comment_length'] = h_eli5_lens

h_rand_lens = human_random_sample['body'].map(lambda x: len(str(x).split())) 
human_random_sample['comment_length'] = h_rand_lens

h_uo_lens = human_uo_sample['body'].map(lambda x: len(str(x).split())) 
human_uo_sample['comment_length'] = h_uo_lens


# comments with quotes
p = r"\s*>.[\sA-Za-z\d\"\']+|\s*\&gt;"

# find
#dd_quotes = dd_sample['body'].apply(lambda x: len(re.findall(p,x)))
# apply to data
#dd_sample['n_of_quotes'] = dd_quotes

#random_quotes = random_sample['body'].apply(lambda x: len(re.findall(p,x)))
# apply to data
#random_sample['n_of_quotes'] = random_quotes

h_eli5_quotes = human_eli5_sample['body'].apply(lambda x: len(re.findall(p,x)))
human_eli5_sample['n_of_quotes'] = h_eli5_quotes

h_rand_quotes = human_random_sample['body'].apply(lambda x: len(re.findall(p,x)))
human_random_sample['n_of_quotes'] = h_rand_quotes

h_uo_quotes = human_uo_sample['body'].apply(lambda x: len(re.findall(p,x)))
human_uo_sample['n_of_quotes'] = h_uo_quotes
# contains links

#%%
#dd_links = dd_sample['body'].str.contains("(http", regex=False)
#dd_sample['contains_links'] = dd_links

#random_links = random_sample['body'].str.contains("(http", regex=False)
#random_sample['contains_links'] = random_links

h_eli5_links = human_eli5_sample['body'].str.contains("(http", regex=False)
human_eli5_sample['contains_links'] = h_eli5_links

h_rand_links = human_random_sample['body'].str.contains("(http", regex=False)
human_random_sample['contains_links'] = h_rand_links

h_uo_links = human_uo_sample['body'].str.contains("(http", regex=False)
human_uo_sample['contains_links'] = h_uo_links

# %%
# paragraphs
#dd_parag = dd_sample['body'].str.contains("\n", regex=False)
#dd_sample['contains_paragraphs'] = dd_parag

#random_parag = random_sample['body'].str.contains("\n", regex=False)
#random_sample['contains_paragraphs'] = random_parag

h_eli5_parag = human_eli5_sample['body'].str.contains("\n", regex=False)
human_eli5_sample['contains_paragraphs'] = h_eli5_parag

h_rand_parag = human_random_sample['body'].str.contains("\n", regex=False)
human_random_sample['contains_paragraps'] = h_rand_parag

h_uo_parag = human_uo_sample['body'].str.contains("\n", regex=False)
human_uo_sample['contains_paragraphs'] = h_uo_parag

#%%
# save the features 
dd_sample.to_csv(here("data/work/samples/data_driven_4000_sample.tsv"), sep='\t')
random_sample.to_csv(here("data/work/samples/random_4000_sample.tsv"), sep='\t')

human_eli5_sample.to_csv(here("data/work/human-annotated-collected-eli5.tsv"), sep='\t')
human_random_sample.to_csv(here("data/work/human-annotated-collected-random.tsv"), sep='\t')
human_uo_sample.to_csv(here("data/work/human-annotated-collected-unpopular.tsv"), sep='\t')

