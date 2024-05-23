# %% imports
import pandas as pd
import math
from datetime import datetime
import matplotlib.pyplot as plt
from readability import Readability
import nltk
nltk.download('punkt')


# %% paths
data_path = "../../data/work/samples/"
cmw_comments_sample_path = data_path + "cmw_comments_sample_1_delta_annotation.tsv"
cmw_submissions_sample_path = data_path + "cmw_submissions_sample_1.tsv"
cwm_delta_thread_sample = data_path + "cmw_comments_sample_1_deltas_thread.tsv"
#####################


# %% load cmw sample
df_comments = pd.read_csv(cmw_comments_sample_path, sep='\t')
df_submissions = pd.read_csv(cmw_submissions_sample_path, sep='\t')
df_delta_threads = pd.read_csv(cwm_delta_thread_sample, sep='\t')
#####################


# %% readability score
import textstat
delta_comments_score = []
delta_comments_tokens = []

non_delta_comments_score = []
non_delta_comments_tokens = [] 
for _, row in df_comments[df_comments['delta'] == True].iterrows():
    if len(str(row['body']).split()) > 120: 
        #r = Readability(row['body'])
        score = textstat.flesch_reading_ease(row['body'])
        if score > 0:
            delta_comments_score.append(score)
            delta_comments_tokens.append(str(row['body']).split())
        #print(r.flesch_kincaid())

for _, row in df_comments[df_comments['delta'] == False].iterrows():
    if len(str(row['body']).split()) > 120: 
        #r = Readability(row['body'])
        score = textstat.flesch_reading_ease(row['body'])
        if score > 0:
            non_delta_comments_score.append(score)
            non_delta_comments_tokens.append(str(row['body']).split())
        #print(r.flesch_kincaid())        


# %%
from scipy.stats import norm
import numpy as np

readability_scores1 = delta_comments_score
readability_scores2 = non_delta_comments_score
# Combine readability scores for easier curve fitting and plotting
all_scores = readability_scores1 + readability_scores2

# Plot overlayed histograms
plt.figure(figsize=(10, 6))

counts1, bins1, _ = plt.hist(readability_scores1, bins=100, alpha=0.5, color='blue', edgecolor='black', label='Delta awarded comments', density=True)
counts2, bins2, _ = plt.hist(readability_scores2, bins=100, alpha=0.5, color='red', edgecolor='black', label='Non-delta awarded comments', density=True)

# Fit Gaussian distributions to the data
mu1, std1 = norm.fit(readability_scores1)
mu2, std2 = norm.fit(readability_scores2)

# Plot the fitted curves
x = np.linspace(min(all_scores), max(all_scores), 100)
p1 = norm.pdf(x, mu1, std1)
p2 = norm.pdf(x, mu2, std2)

plt.plot(x, p1, 'blue', linewidth=2, label='Fitted curve for delta awarded comments')
plt.plot(x, p2, 'red', linewidth=2, label='Fitted curve for non-delta awarded comments')

plt.title('Histogram and Fitted Curves of Readability Scores')
plt.xlabel('Flesch-Kincaid scale')
plt.ylabel('Density')
plt.legend()

# Show the plot
plt.show()

# %%
