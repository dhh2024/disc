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
for _,row in df_comments.iterrows():
    print(row['id'])
    try:
        comment_time = datetime.strptime(str(row['created_utc']), '%Y-%m-%d %H:%M:%S')
        submission_time = datetime.strptime(df_submissions[df_submissions['id'] == row.link_id]['created_utc'].squeeze(), '%Y-%m-%d %H:%M:%S')
        time_taken_to_reply.append((comment_time - submission_time))
    except: 
        time_taken_to_reply.append(None)    
df_comments['time_taken_to_reply'] = time_taken_to_reply

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
df_comments['time_taken_to_reply'].hist(bins = 100, cumulative = True)


# %%
df_comments[df_comments['delta'] == True]['time_taken_to_reply'].hist(bins = 100, cumulative = True)

# %% Find how many comments there were before delta awarded comment

id_posts_delta_comments =  df_comments[df_comments['delta'] == True]['link_id']
df_submissions[(df_submissions['id'] == row.link_id)] #& (df_submissions['time_taken_to_reply'] )
df_comments.groupby
df_comments[df_comments['created_utc'] < df_comments[df_comments['delta'] == True]['created_utc']]

# %%
df_comments.to_csv('../../data/work/samples/cmw_comments_sample_1_delta_annotation.tsv', sep='\t')

# %%
