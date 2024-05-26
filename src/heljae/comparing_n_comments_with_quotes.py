# compare number comments with quotes in cmv comments and random comments
# 
from hereutil import here, add_to_sys_path
add_to_sys_path(here())

expression = r"\s*>.[\sA-Za-z\d\"\']+|\s*\&gt;"

import pandas as pd

cmv_comments = pd.read_csv(here("data/work/samples/cmw_comments_sample_1.tsv"), sep="\t")
# filter out posts whose text is deleted
cmv_comments = cmv_comments[~cmv_comments['body'].str.contains(r"\[removed\]|\[deleted\]|your comment has been removed", na=True)]
n_cmv = cmv_comments.shape[0] # number of filtered comments

rand_comments = pd.read_csv(here("data/work/samples/random_sample_comments_sample_1.tsv"), sep="\t")
rand_comments = rand_comments[~rand_comments['body'].str.contains(r"\[removed\]|\[deleted\]|your comment has been removed", na=True)]
n_rand = rand_comments.shape[0]

aita_comments = pd.read_csv(here("data/work/samples/aita_comments_sample_1.tsv"), sep="\t")
aita_comments = aita_comments[~aita_comments['body'].str.contains(r"\[removed\]|\[deleted\]|your comment has been removed", na=True)]
n_aita = aita_comments.shape[0]

# search for quotes
c = cmv_comments.body.str.contains(expression)
# if there are NA's, assume there was no quote
c = c.fillna(False)
c = cmv_comments.loc[c]

r = rand_comments.body.str.contains(expression)
r = r.fillna(False)
r = rand_comments.loc[r]

a = aita_comments.body.str.contains(expression)
a = a.fillna(False)
a = aita_comments.loc[a]

# create a simple table to visualize
df=pd.DataFrame({"cmv": [c.shape[0], c.shape[0]/n_cmv*100], "aita":[a.shape[0], a.shape[0]/n_aita*100], "random": [r.shape[0], r.shape[0]/n_rand*100]} )
df.index = ["number of comments containing a quote", "% of all posts"]
print(df)
