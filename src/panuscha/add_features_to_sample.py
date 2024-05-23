# %% imports
import pandas as pd
import math
from datetime import datetime
import matplotlib.pyplot as plt
import nltk
nltk.download('punkt')
from PassivePySrc import PassivePy
!python3 -m spacy download en_core_web_lg
passivepy = PassivePy.PassivePyAnalyzer(spacy_model = "en_core_web_lg")


# %% paths
data_path = "../../data/work/samples/"
data_driven_4000_sample_path = data_path + 'data_driven_4000_sample.tsv' #'random_sample_comments_sample_1.tsv'

# %% load power_users
df_data_driven_sample = pd.read_csv(data_driven_4000_sample_path , sep='\t')


# %%
df_data_driven_sample = passivepy.match_corpus_level(df_data_driven_sample, column_name='body', full_passive=True, truncated_passive=True)

# %% readability function
import textstat
def readability(body):
    try:
        #if len(body.split()) > 20:
            score = textstat.flesch_reading_ease(body)
            if score > 0:
                return score
            else:
                return None    
        #else:
        #    return None               
    except:
        return None 
    
# %% readability function    
df_data_driven_sample = df_data_driven_sample.rename(columns={"document": "body"})
df_data_driven_sample['readability_score'] = df_data_driven_sample['body'].apply(lambda x: readability(str(x)))
#df_random_sample = df_random_sample[df_random_sample['readability_score'].notna()]

# %%

df_data_driven_sample.to_csv('../../data/work/samples/data_driven_4000_sample.tsv', sep='\t')
df_data_driven_sample.head()

# %%
%load_ext autoreload
%autoreload 2
from hereutil import here, add_to_sys_path
add_to_sys_path(here()) # noqa
from src.common_basis import *

# %% paths
eng, con = get_db_connection()
cmv_comments = pd.read_sql_table('cmw_comments_a', con, columns=['id', 'parent_comment_id'])

# %%
cmv_comments['id'] = cmv_comments['id'].apply(lambda x: int(x))
cmv_comments['parent_comment_id'] = cmv_comments['parent_comment_id'].apply(lambda x: None if math.isnan(x) else int(x))

# %%
def find_number_of_comments(parent_comment_id):
    number_of_replies = 0
    while math.isnan(parent_comment_id) == False:
            parent_comment_id = cmv_comments[cmv_comments['id'] == parent_comment_id]['parent_comment_id'].squeeze()
            number_of_replies += 1
    print(number_of_replies)        
    return number_of_replies

replies = df_data_driven_sample['parent_comment_id'].apply(lambda x: int(find_number_of_comments(x)))

#

# %%
df_data_driven_sample['number_of_replies_before_comment'] = replies

# %%
df_data_driven_sample.to_csv('../../data/work/samples/data_driven_4000_sample.tsv', sep='\t')


# %%
def after_comment_thread(id):
    replies = cmv_comments[cmv_comments['parent_comment_id'] == id]
    if replies.empty:
        return [id]
    else:
        ids = [id]
        for _, row in replies.iterrows():
            ids.extend(after_comment_thread(row['id']))
        return ids  
    
df_data_driven_sample['comments_after_thread'] = df_data_driven_sample['id'].apply(lambda x: len(after_comment_thread(x)) - 1)


# %%
df_data_driven_sample.head()
# %%
df_data_driven_sample = df_data_driven_sample.drop(columns=['Unnamed: 0', 'Unnamed: 0.1'])

# %%
df_data_driven_sample.head()

# %%
df_data_driven_sample.columns

# %%
df_data_driven_sample.to_csv('../../data/work/samples/data_driven_4000_sample.tsv', sep='\t')

# %%
