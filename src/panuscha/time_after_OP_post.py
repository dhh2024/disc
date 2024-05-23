# %% imports
import pandas as pd
import math
from datetime import datetime
import matplotlib.pyplot as plt

# %% paths
data_path = "../../data/work/samples/"
cmw_comments_sample_path = data_path + "cmw_comments_sample_1_delta_annotation.tsv"
cmw_submissions_sample_path = data_path + "cmw_submissions_sample_1.tsv"
#####################




# %% download cmw sample
df_comments = pd.read_csv(cmw_comments_sample_path, sep='\t')
df_submissions = pd.read_csv(cmw_submissions_sample_path, sep='\t')


# %% download cmw sample
df_comments['id']

# %% download cmw sample
# in case we want to discard deleted comments
#df_comments = df_comments.drop(df_comments[df_comments.body == '[deleted]'].index)
df_comments['parent_comment_id'].apply(lambda x: int(x) if math.isnan(x) == False else None)

######################

# %% compute how much time it took to reply to the comment
df_comments.created_utc
time_taken_to_reply = []
time_taken_to_reply_minutes = []
for _,row in df_comments.iterrows():
    print(row['id'])
    try:
        comment_time = datetime.strptime(str(row['created_utc']), '%Y-%m-%d %H:%M:%S')
        submission_time = datetime.strptime(df_submissions[df_submissions['id'] == row.link_id]['created_utc'].squeeze(), '%Y-%m-%d %H:%M:%S')
        time_taken_to_reply.append((comment_time - submission_time))
        time_taken_to_reply_minutes.append((comment_time - submission_time).total_seconds()/60)
    except: 
        time_taken_to_reply.append(None)  
        time_taken_to_reply_minutes.append(None)    
df_comments['time_taken_to_reply'] = time_taken_to_reply
df_comments['time_taken_to_reply_minutes'] = time_taken_to_reply_minutes

# %% plot time taken to reply
# Calculate time intervals (in seconds) for replies
# Create a plot
plt.figure(figsize=(10, 5))
plt.plot(time_taken_to_reply, marker='o')
plt.title('Time to Reply to Comments')
plt.xlabel('Comment Index')
plt.ylabel('Time to Reply (seconds)')
plt.grid(True)
plt.show()


# %%
df_comments[df_comments['delta'] == True]['time_taken_to_reply']



# %%
ax1 = df_comments[df_comments['time_taken_to_reply_minutes']< 1400]['time_taken_to_reply_minutes'].hist(bins = 100)
ax1.set_title('Interval between OP submission and delta awarded comment post')


# %%
import statistics
ax2 = df_comments[(df_comments['delta'] == True) & (df_comments['time_taken_to_reply_minutes']< 1400)]['time_taken_to_reply_minutes'].hist(bins = 200)
ax2.set_title('Time interval between OP submission and delta awarded comment post one day ')
ax2.set_xlabel('Minutes')
ax2.set_ylabel('Number of comments')

# %% Find how many comments there were before delta awarded comment
id_posts_delta_comments =  df_comments[df_comments['delta'] == True]['link_id']
df_submissions[(df_submissions['id'] == row.link_id)] #& (df_submissions['time_taken_to_reply'] )
df_comments.groupby
df_comments[df_comments['created_utc'] < df_comments[df_comments['delta'] == True]['created_utc']]

# %%
df_comments.to_csv('../../data/work/samples/cmw_comments_sample_1_delta_annotation.tsv', sep='\t')

# %%
# How many comments were there posted before the delta awarded
def how_many_comments_before_delta(link_id, delta_interval):
    #print(df_comments[(df_comments['time_taken_to_reply_minutes'] < delta_interval) & (df_comments['link_id'] == link_id) ])
    #print(df_comments[(df_comments['link_id'] == link_id) ])
    return len(df_comments[(df_comments['time_taken_to_reply_minutes'] < delta_interval) & (df_comments['link_id'] == link_id) ])


comments_before_delta = []
for _, row in df_comments[df_comments['delta'] == True].iterrows():
    comments_before_delta.append(how_many_comments_before_delta(row['link_id'], row['time_taken_to_reply_minutes']))

# %%
comments_before_delta
# %%
df_comments['comments_before_delta'] = None
df_comments.loc[df_comments['delta'] == True, 'comments_before_delta'] =  comments_before_delta


# %%
df_comments[df_comments['delta'] == True]['comments_before_delta']

# %%
less_then = list(filter(lambda x: x < 200, comments_before_delta))
plt.hist( less_then, bins=100, edgecolor='black')
plt.title('Comments before delta awarded comment')
plt.xlabel('Number of comments before delta was awarded')


# %%
min(comments_before_delta)
# %%
