# %% imports
import pandas as pd
import math

# %% paths
data_path = "../../data/work/samples/"
cmw_comments_sample_path = data_path + "cmw_comments_sample_1.tsv"
cmw_submissions_sample_path = data_path + "cmw_submissions_sample_1.tsv"
#####################


# %% download cmw sample
df_comments = pd.read_csv(cmw_comments_sample_path, sep='\t')

# in case we want to discard deleted comments
#df_comments = df_comments.drop(df_comments[df_comments.body == '[deleted]'].index)
df_comments['parent_comment_id'].apply(lambda x: int(x) if math.isnan(x) == False else None)
df_submissions = pd.read_csv(cmw_submissions_sample_path, sep='\t')
######################


# %% number of comments per submission
number_of_comments = df_comments['link_id'].value_counts()
print(number_of_comments)
ax = number_of_comments.hist(bins=100)
ax.set_title('Number of comments per submission')
ax.set_xlabel('Number of comments')
########################


# %% number of tokens per comments

#add column that counts number of tokens
df_comments['number_of_tokens'] = df_comments['body'].apply(lambda x: len(str(x).split())) 
number_of_tokens = df_comments['number_of_tokens']
print(number_of_tokens)

#create histogram
ax = number_of_tokens.hist(bins=1000)
ax.set_title('Number of tokens per submission')
#########################



# %% number of ∆ 

#anotate deltabot awarding comment
df_comments['bot_delta_awarded'] = df_comments[(df_comments['author'] == 'DeltaBot')]['body'].apply(lambda x: 'Confirmed: 1 delta awarded to' in x)

#set all to False
df_comments['bot_delta_awarded'] = df_comments['bot_delta_awarded'].apply(lambda x: False if math.isnan(x) else True)

#ids of OP's comments that they are awarding delta
op_delta_comments_id = df_comments[df_comments['bot_delta_awarded'] == True]['parent_comment_id']

#∆ awarding comment
delta_awarded_comments_id = df_comments[df_comments['id'].isin(op_delta_comments_id)]['parent_comment_id']
print(delta_awarded_comments_id)
#########################



# %% ∆ awarded comments 
#df_comments = df_comments.dropna()
df_comments['delta'] = False
df_comments.loc[df_comments['id'].isin(delta_awarded_comments_id ), 'delta'] = True
df_comments[df_comments['delta'] == True]
df_comments.to_csv('../../data/work/samples/cmw_comments_sample_1_delta_annotation.tsv', sep='\t')


# %% number of tokens in delta awarded comments
number_of_tokens_in_delta_award = df_comments[df_comments['id'].isin(delta_awarded_comments_id)]['number_of_tokens']
ax = number_of_tokens_in_delta_award.hist(bins = 100)
ax.set_title('Number of tokens in delta awarded comments')
#########################


# %% number of ∆ awarded comments without any discussion
number_of_delta_without_thread = sum(df_comments[(df_comments['id'].isin(delta_awarded_comments_id))]['parent_comment_id'].isna())
number_of_delta_with_thread = len(df_comments[(df_comments['id'].isin(delta_awarded_comments_id))]['parent_comment_id']) - number_of_delta_without_thread

print('Number of delta awarded comments without discussion: ' + str(number_of_delta_without_thread))
print('Number of delta awarded comments with discussion: ' + str(number_of_delta_with_thread))
#########################



# %% number of replies in delta awarded thread
print(len(delta_awarded_comments_id))
def find_number_of_comments(parent_comment_id):
    number_of_replies = 0
    while math.isnan(parent_comment_id) == False:
            parent_comment_id = df_comments[df_comments['id'] == parent_comment_id]['parent_comment_id'].squeeze()
            number_of_replies += 1
    return number_of_replies

df_comments['number_of_replies'] = df_comments[df_comments['id'].isin(delta_awarded_comments_id)]['parent_comment_id'].apply(lambda x: int(find_number_of_comments(x)))
#########################


# %% Visualize number of comments before delta was awarded
df_number_of_replies = df_comments['number_of_replies'].dropna()
print(len(df_number_of_replies))
ax = df_number_of_replies.hist(bins = 20)
ax.set_title('Number of replies in threads')
df_number_of_replies.value_counts()
#########################


# %% 

# function that returns id's of comments that are in  threads
def return_thread(parent_comment_id):
    thread = []
    print(parent_comment_id)
    while math.isnan(parent_comment_id) == False:
        thread.append(parent_comment_id)
        parent_comment_id = df_comments[df_comments['id'] == parent_comment_id]['parent_comment_id'].squeeze()
    return thread
#########################


threads = set(delta_awarded_comments_id)
for i, row in df_comments[df_comments['id'].isin(delta_awarded_comments_id)].iterrows():
    thread = return_thread(row['parent_comment_id'])  
    threads.update(thread)
#########################


# %%
df_comments_delta = df_comments[df_comments['id'].isin(threads)]
df_comments_delta.to_csv('../../data/work/samples/cmw_comments_sample_1_deltas_thread.tsv', sep='\t')
#########################


# %%
df_comments_delta = df_comments[df_comments['id'].isin(threads)]
df_comments[df_comments['delta'] == True].to_csv('../../data/work/samples/cmw_comments_sample_1_only_delta_comments.tsv', sep='\t')

# %%
df_comments.to_csv('../../data/work/samples/cmw_comments_sample_1_delta_annotation.tsv', sep='\t')


# %%
