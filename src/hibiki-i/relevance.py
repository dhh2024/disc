#%% ###################################################
# !pip install -U sentence-transformers


# %% ###################################################
# encode text and save vectors
########################################################

# from hereutil import here, add_to_sys_path
# add_to_sys_path(here())
# import pandas as pd
# import numpy as np
# from sentence_transformers import SentenceTransformer

# model = SentenceTransformer('multi-qa-distilbert-cos-v1')

# dfs = pd.read_csv(here('data/work/samples/cmw_submissions_sample_1.tsv'), sep='\t')
# dfs['body'] = dfs['title'] + ' ' + dfs['selftext']
# print('submissions loaded')
# dfs['embeddings'] = dfs['body'].apply(lambda text: model.encode(text))
# print('submissions encoded')
# dfs.to_csv(here('data/work/samples/cmw_submissions_sample_1_embeddings.tsv'), sep='\t', index=False)
# print('submissions saved')

# dfc = pd.read_csv(here('data/work/samples/cmw_comments_sample_1.tsv'), sep='\t')
# print('comments loaded')
# dfc['embeddings'] = dfc['body'].apply(lambda text: model.encode(str(text)))
# print('comments encoded')
# dfc.to_csv(here('data/work/samples/cmw_comments_sample_1_embeddings.tsv'), sep='\t', index=False)
# print('comments saved')

# sub_vecs = dfs['embeddings']
# com_vecs = dfc['embeddings']

# sub_vecs = pd.DataFrame(sub_vecs.to_list())
# com_vecs = pd.DataFrame(com_vecs.to_list())

# sub_vecs.index = dfs['id']
# com_vecs.index = dfc['id']

# sub_vecs.to_csv(here('data/work/samples/cmw_submissions_sample_1_vectors.tsv'), sep='\t', index=True)
# com_vecs.to_csv(here('data/work/samples/cmw_comments_sample_1_vectors.tsv'), sep='\t', index=True)


# %% ###################################################
# compute consistency and relevance
########################################################

from hereutil import here, add_to_sys_path
add_to_sys_path(here())
import pandas as pd
import numpy as np

def cosine(u, v):
    return np.dot(u, v) / (np.linalg.norm(u) * np.linalg.norm(v))

# load data
ds = pd.read_csv(here('data/work/samples/cmw_submissions_sample_1.tsv'), sep='\t')
dfc = pd.read_csv(here('data/work/samples/cmw_comments_sample_1.tsv'), sep='\t')
sub_vecs = pd.read_csv(here('data/work/samples/cmw_submissions_sample_1_vectors.tsv'), sep='\t', index_col=0)
com_vecs = pd.read_csv(here('data/work/samples/cmw_comments_sample_1_vectors.tsv'), sep='\t', index_col=0)

com_vecs_list = [row for row in com_vecs.to_numpy()]
com_vecs = pd.Series(com_vecs_list, index=com_vecs.index)
sub_vecs_list = [row for row in sub_vecs.to_numpy()]
sub_vecs = pd.Series(sub_vecs_list, index=sub_vecs.index)

# clean data
dfc = dfc[dfc['body'].notna()]

# compute consistencies
def compute_consistency(submission):
    submission_vec = sub_vecs[submission['id']]
    comments = dfc[dfc['link_id']==submission['id']]
    comments_vecs = com_vecs[comments['id']]
    distances = comments_vecs.apply(lambda vec: cosine(submission_vec, vec))
    return distances.mean()

ds['consistency'] = ds.apply(compute_consistency, axis=1)
ds[['id','consistency']].to_csv(here('data/work/samples/cmw_submissions_sample_1_consistency.tsv'), sep='\t', index=False)

# compute relevance
def compute_relevance(comment):
    comment_vec = com_vecs[comment['id']]
    parent_comment = dfc[dfc['id']==comment['parent_comment_id']]
    if parent_comment.empty:
        parent_vec = sub_vecs[comment['link_id']]
    else:    
        parent_vec = com_vecs[parent_comment['id']].values[0]     
    return cosine(comment_vec, parent_vec)

dfc['relevance'] = dfc.apply(compute_relevance, axis=1)
dfc[['id', 'relevance']].to_csv(here('data/work/samples/cmw_comments_sample_1_relevance.tsv'), sep='\t', index=False)

# %% ###################################################
# plot results
########################################################


import matplotlib.pyplot as plt
import seaborn as sns

#######################################################
# limit to civil discussions
# discussion exists
dfs = ds[ds['num_comments']>2] 
deleted_sub_id = set()
for i, row in dfc.iterrows(): # no deleted comments in the thread
    if row['body'] == '[deleted]':
        deleted_sub_id.add(row['link_id'])
dfs = dfs[-dfs['id'].isin(deleted_sub_id)]

# delta awarded or not
delta_sub_id = set()
for i, row in dfc.iterrows():
    if row['author'] == "DeltaBot" and row['body'].startswith('Confirmed'):
        delta_sub_id.add(row['link_id'])
dfs_delta = dfs[dfs['id'].isin(delta_sub_id)]
dfs_nodelta = dfs[-dfs['id'].isin(delta_sub_id)]

dfc_delta = dfc[dfc['link_id'].isin(set(dfs_nodelta['id'].to_list()))]
dfc_nodelta = dfc[dfc['link_id'].isin(set(dfs_delta['id'].to_list()))]

#######################################################
# visualise
# long discussions vs short discussions
median = dfs_delta['num_comments'].median()
dfsl = dfs_delta[dfs_delta['num_comments']>median] # long discussions
dfss = dfs_delta[dfs_delta['num_comments']<=median] # short discussions

sns.kdeplot(dfsl['consistency'], color='blue', label='long discussions', alpha=0.5, fill=True)
sns.kdeplot(dfss['consistency'], color='red', label='short discussions', alpha=0.5, fill=True)
plt.xlabel('Consistency')
plt.legend(loc='upper right')
plt.title('average cosine similairty between submission and comments')
plt.show()

# delta vs no delta
sns.kdeplot(dfs['consistency'], color='blue', label='delta', alpha=0.5, fill=True)
sns.kdeplot(dfs_nodelta['consistency'], color='red', label='no delta', alpha=0.5, fill=True)
plt.xlabel('Consistency')
plt.legend(loc='upper right')
plt.title('average cosine similairty between submission and comments')
plt.show()

# number of comments vs consistency
sns.scatterplot(data=dfs, x='num_comments', y='consistency')
plt.xlabel('Number of comments')
plt.ylabel('Consistency')
plt.title('num_comments vs consistency')
plt.show()

# number of unique participants vs consistency
num_participants = dfc.groupby('link_id')['author'].nunique().rename('num_participants').reset_index()
dfs2 = dfs.copy()
dfs2 = dfs2.merge(num_participants, left_on='id', right_on='link_id')
sns.scatterplot(data=dfs2, x='num_participants', y='consistency')
plt.xlabel('Number of participants')
plt.ylabel('Consistency')
plt.title('num_participants vs consistency')
plt.show()

# inspect outliers
outliers = dfs2[dfs2['num_participants']>200]

def compute_similarities(submission):
    submission_vec = sub_vecs[submission['id']]
    comments = dfc[dfc['link_id']==submission['id']]
    comments_vecs = com_vecs[comments['id']]
    distances = comments_vecs.apply(lambda vec: cosine(submission_vec, vec))
    return distances.values

outliers['similarities'] = outliers.apply(compute_similarities, axis=1)
for i, row in outliers.iterrows():
    sns.histplot(row['similarities'], bins=50, kde=True, label='id ' + str(row['id']))
plt.xlabel('Cosine similarity') 
plt.legend(loc='upper right')
plt.title('similarity distribution of outliers')
plt.show()

# plot distribution of relevance and consistency
sns.kdeplot(dfc['relevance'], color='blue', alpha=0.5, fill=True)
plt.xlabel('Relevance')
plt.title('cosine similarity between comment and parent')
plt.show()

sns.kdeplot(dfs['consistency'], color='red', alpha=0.5, fill=True)
plt.xlabel('Consistency')
plt.title('consistency between submission and comments')
plt.show()
# %%
