#%% ###################################################
# !pip install -U sentence-transformers


#%% ######################################################
# encode submissions & comments containing stop-arguing interventions
##########################################################

from hereutil import here, add_to_sys_path
add_to_sys_path(here())
from src.common_basis import get_db_connection
from sqlalchemy import text
_, con = get_db_connection()

import pandas as pd
import numpy as np



samuli = pd.read_csv(here('data/work/annotations/interventions-samuli-2.csv'))
ylva = pd.read_csv(here('data/work/annotations/interventions-ylva-2.csv'))
samuli_yes = samuli[samuli['intervention']=='Yes']
ylva_yes = ylva[ylva['intervention']=='Yes']
intervention_ids = pd.concat([samuli_yes['link_id'], ylva_yes['link_id']])
intervention_ids.drop_duplicates(inplace=True)
intervention_ids.dropna(inplace=True)
intervention_ids = list(map(str, intervention_ids))

dfs = pd.read_sql(text("""
                       SELECT *
                       FROM cmw_submissions_a
                       WHERE id IN :intervention_ids
                       """), con, params={'intervention_ids': intervention_ids}
)

dfs.to_csv(here('data/work/genuine_interventions_submissions.tsv'), sep='\t', index=False)

dfs['body'] = dfs['title'] + ' ' + dfs['selftext']

sub_ids = list(map(str, dfs['id']))

dfc = pd.read_sql(text("""
                       SELECT *
                       FROM cmw_comments_a
                       WHERE link_id IN :sub_ids
                       """), con, params={'sub_ids': sub_ids}
)

dfc.to_csv(here('data/work/genuine_interventions_comments.tsv'), sep='\t', index=False)

from sentence_transformers import SentenceTransformer

model = SentenceTransformer('multi-qa-distilbert-cos-v1')

sub_vecs = dfs['body'].apply(lambda text: model.encode(str(text)))
sub_vecs2 = pd.DataFrame(sub_vecs.tolist())
sub_vecs2.index = dfs['id']
sub_vecs2.to_csv(here('data/work/genuine_interventions_submissions_vectors.tsv'), sep='\t')


# encode 100 comments at a time (took too long, interrupted when 13200 comments are encoded)
import time
n = dfc.shape[0]
cnt = 1
start = time.time()

for i in range(0, n, 100):
    temp = dfc.iloc[i:i+100, :]
    com_vecs = temp['body'].apply(lambda text: model.encode(str(text), show_progress_bar=False))
    com_vecs = pd.DataFrame(com_vecs.tolist())
    com_vecs.index = temp['id']
    com_vecs.to_csv(here(f'data/work/vecs/vecs_{cnt}.tsv'), sep='\t')
    print(f'{cnt}: {i+100} comments done in {(time.time()-start) // 60} min')
    cnt += 1

vecs = pd.DataFrame()
for i in range(1, cnt):
    temp = pd.read_csv(here(f'data/work/vecs/vecs_{i}.tsv'), sep='\t', index_col=0)
    vecs = pd.concat([vecs, temp])
vecs.to_csv(here('data/work/genuine_interventions_comments_vectors.tsv'), sep='\t')



#%% ######################################################
# calculate consistency and relevance
##########################################################


from hereutil import here, add_to_sys_path
add_to_sys_path(here())
import pandas as pd
import numpy as np

def cosine(u, v):
    return np.dot(u, v) / (np.linalg.norm(u) * np.linalg.norm(v))

# load data
dfs = pd.read_csv(here('data/work/genuine_interventions_submissions.tsv'), sep='\t')
dfc = pd.read_csv(here('data/work/genuine_interventions_comments.tsv'), sep='\t')

sub_vecs = pd.read_csv(here('data/work/genuine_interventions_submissions_vectors.tsv'), sep='\t', index_col=0)
com_vecs = pd.read_csv(here('data/work/genuine_interventions_comments_vectors.tsv'), sep='\t', index_col=0)

com_vecs_list = [row for row in com_vecs.to_numpy()]
com_vecs = pd.Series(com_vecs_list, index=com_vecs.index)
sub_vecs_list = [row for row in sub_vecs.to_numpy()]
sub_vecs = pd.Series(sub_vecs_list, index=sub_vecs.index)

# clean data
dfc = dfc[dfc['body'].notna()]

# compute consistencies
def compute_consistency(submission):
    submission_vec = sub_vecs[submission['id']]
    comments = dfc[(dfc['link_id']==submission['id']) & (dfc['id'].isin(com_vecs.index))]
    comments_vecs = com_vecs[comments['id']]
    distances = comments_vecs.apply(lambda vec: cosine(submission_vec, vec))
    return distances.mean()

dfs['consistency'] = dfs.apply(compute_consistency, axis=1)
dfs[['id','consistency']].to_csv(here('data/work/genuine_interventions_submissions_consistency.tsv'), sep='\t', index=False)

# compute relevance
def compute_relevance(comment):
    if comment['id'] not in com_vecs.index:
        return np.nan
    comment_vec = com_vecs[comment['id']]
    parent_comment = dfc[dfc['id']==comment['parent_comment_id']]
    if parent_comment.empty:
        parent_vec = sub_vecs[comment['link_id']]
    else:    
        parent_vec = com_vecs[parent_comment['id']].values[0]     
    return cosine(comment_vec, parent_vec)

dfc['relevance'] = dfc.apply(compute_relevance, axis=1)
dfc[['id', 'relevance']].to_csv(here('data/work/genuine_interventions_comments_relevance.tsv'), sep='\t', index=False)


#%% ######################################################
# plot results
##########################################################

from hereutil import here, add_to_sys_path
add_to_sys_path(here())
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# load data
dfs = pd.read_csv(here('data/work/genuine_interventions_submissions_consistency.tsv'), sep='\t')
dfc = pd.read_csv(here('data/work/genuine_interventions_comments_relevance.tsv'), sep='\t')
dfs_rnd = pd.read_csv(here('data/work/samples/cmw_submissions_sample_1_consistency.tsv'), sep='\t')
dfc_rnd = pd.read_csv(here('data/work/samples/cmw_comments_sample_1_relevance.tsv'), sep='\t')

dfs.dropna()
dfc.dropna()

# consistency
sns.kdeplot(dfs['consistency'], color='red', alpha=0.5, fill=True, label='interventions')
sns.kdeplot(dfs_rnd['consistency'], color='blue', alpha=0.5, fill=True, label='random')
plt.xlabel('Consistency')
plt.title('threads with genuine interventions vs random threads')
plt.legend()
plt.show()

# relevance
sns.kdeplot(dfc['relevance'], color='red', alpha=0.5, fill=True, label='stop-arguing')
sns.kdeplot(dfc_rnd['relevance'], color='blue', alpha=0.5, fill=True, label='random')
plt.xlabel("comments' relevance to parent comments")
plt.title('threads with genuine interventions vs random threads')
plt.legend()
plt.show()
# %%
