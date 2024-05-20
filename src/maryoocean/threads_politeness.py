# %% [markdown]
# Started to look into this on Friday and got time only to load the subsets collected by Charlotte. 
#
# **Hypothesis:** the more direct the disagreement is the better it is for the purpose of the subreddit (as opposed to politeness strategies elsewhere). The idea would be to look into the first comments in threads that get deltas vs all others with delta and compare the disagreement strategies.

# %%
import pandas as pd
import stanza
import re
import matplotlib.pyplot as plt
import numpy as np


# %%
def open_df(filename):
    df = pd.read_csv(filename, sep='\t', header=0)
    return df


# %%
threads = open_df("cmw_comments_sample_1_deltas_thread.tsv")
threads_first = threads[(threads['number_of_replies'] == 0.0) & (threads['delta'] == True)]
threads_first_noD = threads[threads['delta'] == False]
threads_deep = threads[(threads['number_of_replies'] > 0.0) & (threads['delta'] == True)]

# %%
threads_first

# %%
threads_first_noD

# %%
threads_deep

# %%
