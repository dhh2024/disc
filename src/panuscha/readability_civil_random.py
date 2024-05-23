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
random_comments_sample_path = data_path + "random_sample_comments_sample_1.tsv"
civility_comments_sample_path = "../../data/work/" + "civility_annotation_set.tsv"
cmw_comments_sample_path = data_path + "cmw_comments_sample_1_delta_annotation.tsv"
eli5_comments_sample_path = data_path + "eli5_comments_sample_1.tsv"
#####################


# %% load cmw sample
df_random_comments = pd.read_csv(random_comments_sample_path, sep='\t')
df_civility = pd.read_csv(civility_comments_sample_path, sep='\t')
df_comments = pd.read_csv(cmw_comments_sample_path, sep='\t')
df_eli5_comments = pd.read_csv(eli5_comments_sample_path, sep='\t')
#####################


# %% readability score
import textstat
from scipy.stats import norm
import numpy as np
random_comments_score = []

civility_comments_score = []

delta_comments_score = []

non_delta_comments_score = []

eli5_comments_score = []

cutoff = 120

for _, row in df_random_comments.iterrows():
    if len(str(row['body']).split()) > cutoff: 
        #r = Readability(row['body'])
        score = textstat.flesch_reading_ease(row['body'])
        if score > 0:
            random_comments_score.append(score)
        #print(r.flesch_kincaid())

for _, row in df_civility .iterrows():
    if len(str(row['body']).split()) > cutoff: 
        #r = Readability(row['body'])
        score = textstat.flesch_reading_ease(row['body'])
        if score > 0:
            civility_comments_score.append(score)
        #print(r.flesch_kincaid())

for _, row in df_comments[df_comments['delta'] == True].iterrows():
    if len(str(row['body']).split()) > cutoff: 
        #r = Readability(row['body'])
        score = textstat.flesch_reading_ease(row['body'])
        if score > 0:
            delta_comments_score.append(score)  

for _, row in df_comments[df_comments['delta'] == False].iterrows():
    if len(str(row['body']).split()) > cutoff: 
        #r = Readability(row['body'])
        score = textstat.flesch_reading_ease(row['body'])
        if score > 0:
            non_delta_comments_score.append(score)   

for _, row in df_eli5_comments.iterrows():
    if len(str(row['body']).split()) > cutoff: 
        #r = Readability(row['body'])
        score = textstat.flesch_reading_ease(row['body'])
        if score > 0:
            eli5_comments_score.append(score)                       




readability_scores1 = random_comments_score
readability_scores2 = delta_comments_score
readability_scores3 = non_delta_comments_score
readability_scores4 = eli5_comments_score
# Combine readability scores for easier curve fitting and plotting
all_scores = readability_scores1 + readability_scores2 + readability_scores3 + readability_scores4

# Plot overlayed histograms
plt.figure(figsize=(10, 6))

counts1, bins1, _ = plt.hist(readability_scores1, bins=100, alpha=0.5, color='green', edgecolor='black', label='Random comments sample', density=True)
counts2, bins2, _ = plt.hist(readability_scores2, bins=100, alpha=0.5, color='blue', edgecolor='black', label='Delta CMV comments sample', density=True)
counts3, bins3, _ = plt.hist(readability_scores3, bins=100, alpha=0.5, color='red', edgecolor='black', label='Non delta CMV comments sample', density=True)
counts4, bins4, _ = plt.hist(readability_scores4, bins=100, alpha=0.5, color='yellow', edgecolor='black', label='ELI5 comments sample', density=True)



# Fit Gaussian distributions to the data
mu1, std1 = norm.fit(readability_scores1)
mu2, std2 = norm.fit(readability_scores2)
mu3, std3 = norm.fit(readability_scores3)
mu4, std4 = norm.fit(readability_scores4)

# Plot the fitted curves
x = np.linspace(min(all_scores), max(all_scores), 100)
p1 = norm.pdf(x, mu1, std1)
p2 = norm.pdf(x, mu2, std2)
p3 = norm.pdf(x, mu3, std3)
p4 = norm.pdf(x, mu4, std4)

plt.plot(x, p1, 'green', linewidth=2, label='Fitted curve for random sample comments')
plt.plot(x, p2, 'blue', linewidth=2, label='Fitted curve for cmv delta awarded sample comments')
plt.plot(x, p3, 'red', linewidth=2, label='Fitted curve for cmv non-delta sample comments')
plt.plot(x, p4, 'yellow', linewidth=2, label='Fitted curve for eli5 sample comments')

plt.title('Histogram and Fitted Curves of Readability Scores')
plt.xlabel('Flesch-Kincaid scale')
plt.ylabel('Density')
plt.legend()

# %%
