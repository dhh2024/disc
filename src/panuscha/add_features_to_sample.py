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
random_4000_sample_path = data_path + 'random_4000_sample.tsv' #'random_sample_comments_sample_1.tsv'

# %% load power_users
df_random_sample = pd.read_csv(random_4000_sample_path , sep='\t')


# %%
df_random_sample = passivepy.match_corpus_level(df_random_sample, column_name='body', full_passive=True, truncated_passive=True)

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
df_random_sample = df_random_sample.rename(columns={"document": "body"})
df_random_sample['readability_score'] = df_random_sample['body'].apply(lambda x: readability(str(x)))
#df_random_sample = df_random_sample[df_random_sample['readability_score'].notna()]

# %%
df_random_sample.head()

# %%

def find_number_of_comments(parent_comment_id):
    number_of_replies = 0
    while math.isnan(parent_comment_id) == False:
            parent_comment_id = df_random_sample[df_random_sample['id'] == parent_comment_id]['parent_comment_id'].squeeze()
            number_of_replies += 1
    return number_of_replies

df_random_sample['number_of_replies'] = df_random_sample['parent_comment_id'].apply(lambda x: int(find_number_of_comments(x)))