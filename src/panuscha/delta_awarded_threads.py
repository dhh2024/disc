
# %% imports
import pandas as pd
import math
from datetime import datetime
import matplotlib.pyplot as plt

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


# %% threads in delta
df_comments['parent_comment_id'].apply(lambda x: int(x) if math.isnan(x) == False else None)
df_delta_threads['parent_comment_id'].apply(lambda x: int(x) if math.isnan(x) == False else None)
def after_delta_thread(id):
    replies = df_comments[df_comments['parent_comment_id'] == id]
    if replies.empty:
        return [id]
    else:
        ids = [id]
        for _, row in replies.iterrows():
            ids.extend(after_delta_thread(row['id']))
        return ids    

discussion_delta = {}
discussion_delta_lengths = {}
for _, row in df_delta_threads[df_delta_threads['parent_comment_id'].isna()].iterrows():
    if not math.isnan(row['id']):
        discussion_delta[row['id']] = after_delta_thread(row['id'])
        discussion_delta_lengths[row['id']] = len(discussion_delta[row['id']])

discussion_all = {}
discussion_all_lengths = {}
for _, row in df_comments[(df_comments['parent_comment_id'].isna()) ].iterrows():
    if not math.isnan(row['id']):
        discussion_all[row['id']] = after_delta_thread(row['id'])   
        discussion_all_lengths[row['id']] = len(discussion_all[row['id']])

discussion_negative_delta = {k: discussion_all[k] for k in discussion_all.keys() if k not in discussion_delta.keys()}   
discussion_negative_delta_lengths = {k: len(discussion_all[k]) for k in discussion_all.keys() if k not in discussion_delta.keys()} 

    # %%



# %%
print(discussion_all_lengths.values())

plt.hist(discussion_all_lengths.values(), bins=200, edgecolor='black', range = [1, 100])
plt.xlabel('Number of comments in thread')
plt.ylabel('Number of threads')
plt.title('Number of comments in thread')

# %%
print(discussion_delta_lengths.values())

plt.hist(discussion_delta_lengths.values(), bins=200, edgecolor='black', range= [3,100])
plt.xlabel('Number of comments in thread')
plt.ylabel('Number of threads')
plt.title('Number of comments in delta thread')



# %%
import statistics
print(statistics.mean(discussion_delta_lengths.values()))
print(statistics.mean(discussion_all_lengths.values()))


# %%

#x = list(filter(lambda x: x>1 and x<50, discussion_delta_lengths.values()))
x = list(discussion_delta_lengths.values())
#y = list(filter(lambda x: x>1 and x<50, discussion_negative_delta_lengths.values()))
y = list(discussion_negative_delta_lengths.values())
#plt.hist([x, y], bins = 200, density = True, label=['Delta awarded threads', 'Non delta awarded threads'],  alpha = 0.5, range = [0, 50], width = 0.3)
plt.hist([x, y], bins=200, density=True, label=['Delta awarded threads', 'Non delta awarded threads'], alpha=0.5, range=[0, 50], width=0.3)






# %% Plot length of delta and non-delta comments
import numpy as np
import matplotlib.pyplot as plt

# Extract values
x = list(discussion_delta_lengths.values())
y = list(discussion_negative_delta_lengths.values())

range = [0, 100]
bins = 100

# Compute histograms without normalization
hist_x, bins_x = np.histogram(x, bins = bins, range=range)
hist_y, bins_y = np.histogram(y, bins = bins, range=range)

# Normalize histograms to get percentage of occurrences
norm_hist_x = hist_x / hist_x.sum()
norm_hist_y = hist_y / hist_y.sum()

# Define the width of the bars
bar_width = 0.5

# Plotting the normalized histograms as bar plots side-by-side
bin_centers_x = 0.5 * (bins_x[1:] + bins_x[:-1])
bin_centers_y = 0.5 * (bins_y[1:] + bins_y[:-1])

# Offset the bin centers for y by the bar width to avoid overlap
bin_centers_y_offset = bin_centers_y + bar_width

plt.bar(bin_centers_x - bar_width / 2, norm_hist_x, width=bar_width, alpha=1, label='Delta awarded threads')
plt.bar(bin_centers_y_offset - bar_width / 2, norm_hist_y, width=bar_width, alpha=1,  label='Non delta awarded threads')

# Adding labels and title
plt.xlabel('Number of Comments')
plt.ylabel('Percentage of Occurrence')
plt.title('Normalized Histogram of Number of Comments')
plt.legend()

# Display the plot
plt.show()





# %%

