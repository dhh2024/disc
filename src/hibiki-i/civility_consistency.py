#%% ###################################################
# !pip install -U sentence-transformers


#%% ######################################################
# format data
##########################################################

from hereutil import here, add_to_sys_path
add_to_sys_path(here())
import pandas as pd
import numpy as np
import re

# load data
kart = pd.read_csv(here('data/work/annotations/civility-kart.csv'), dtype={'selftext': str})
marina = pd.read_csv(here('data/work/annotations/civility-marina.csv'), dtype={'selftext': str})
nan = pd.read_csv(here('data/work/annotations/civility-nan.csv'), dtype={'selftext': str})

# get ratings
criteria = ['dd', 'fm', 'gt', 'im', 'is', 'jc', 'mc', 'md', 'pd']
rating = {'[{"rating":1}]': 1, '[{"rating":2}]': 2, '[{"rating":3}]': 3, '[{"rating":4}]': 4, '[{"rating":5}]': 5, '[{"rating":6}]': 6, '[{"rating":7}]': 7}
kart[criteria] = kart[criteria].replace(rating)
marina[criteria] = marina[criteria].replace(rating)
nan[criteria] = nan[criteria].replace(rating)

# calculate average ratings
kart_rows = kart['body'].dropna()
marina_rows = marina['body'].dropna()
nan_rows = nan['body'].dropna()
rows = pd.concat([kart_rows, marina_rows, nan_rows])
common_rows = rows.drop_duplicates()
common_rows = common_rows[-common_rows.isin(['[deleted]', '[removed]'])]

df = kart[kart['body'].isin(common_rows)][['ancestor_text', 'body'] + criteria]
df[criteria] += marina[marina['body'].isin(common_rows)][criteria]
df[criteria] += nan[nan['body'].isin(common_rows)][criteria]
df[criteria] /= 3
df.dropna(inplace=True)
df.reset_index(drop=True, inplace=True)

# clean ancestor_text
def clean_text(text):
    cleaned_text = re.sub(r'\[?\w+\]? @ \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} :', '', text)
    cleaned_text = re.sub(r'\n+', '', cleaned_text)
    cleaned_text = cleaned_text.strip()
    return cleaned_text
df['ancestor_text'] = df['ancestor_text'].apply(clean_text)



# %% ###################################################
# encode text and calculate relevance, length
########################################################

from sentence_transformers import SentenceTransformer
model = SentenceTransformer('multi-qa-distilbert-cos-v1')


def cosine(u, v):
    return np.dot(u, v) / (np.linalg.norm(u) * np.linalg.norm(v))

# encode body and ancestor_text
df['body_vec'] = df['body'].apply(lambda text: model.encode(str(text)))
df['ancestor_text_vec'] = df['ancestor_text'].apply(lambda text: model.encode(str(text)))

# calculate cosine similarity
df['relevance'] = df.apply(lambda row: cosine(row['body_vec'], row['ancestor_text_vec']), axis=1)

# calculate length
df['body_len'] = df['body'].apply(lambda text: len(text.split()))

# %% ###################################################
# analysis
########################################################

from scipy.stats import pearsonr
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.cluster.hierarchy import linkage, fcluster
import scipy.spatial as sp
from sklearn.metrics.pairwise import cosine_similarity

# correlation between relevance and ratings
results = []
for criterion in criteria:
    corr, p = pearsonr(df['relevance'], df[criterion])
    results.append({'criterion': criterion, 'correlation': corr, 'p': p})
results = pd.DataFrame(results)
plt.bar(results['criterion'], results['correlation'])
plt.xlabel('criterion')
plt.title('correlation between civility and relevance')
plt.show()

# pairplot
sns.pairplot(df[['relevance'] + criteria])
plt.show()

# clustering
distances = 1 - cosine_similarity(df[criteria])
np.fill_diagonal(distances, 0)
mylinkage = linkage(sp.distance.squareform(distances), method='ward')
sns.clustermap(distances, row_linkage=mylinkage, col_linkage=mylinkage)
plt.show()

# there seem to be 4 clusters
cluster = fcluster(mylinkage, 4, criterion='maxclust')
df['cluster'] = cluster

# %%
negative = ['dd', 'is', 'im']
positive = ['relevance', 'fm', 'gt', 'jc', 'mc', 'md', 'pd']
df['civility'] = df[positive].sum(axis=1) - df[negative].sum(axis=1)


# %%
results = []
for criterion in criteria:
    corr, p = pearsonr(df['body_len'], df[criterion])
    results.append({'criterion': criterion, 'correlation': corr, 'p': p})
results = pd.DataFrame(results)
plt.bar(results['criterion'], results['correlation'])
plt.xlabel('criterion')
plt.title('correlation between civility features and comment length')
plt.show()
# %%
