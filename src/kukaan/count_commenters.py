# %% imports
import math
import pandas as pd


# %% paths
data_path = "../../data/work/samples/"
cmw_comments_sample_path = data_path + "cmw_comments_sample_1.tsv"
cmw_submissions_sample_path = data_path + "cmw_submissions_sample_1.tsv"
#####################


# %% download cmw sample
df_submissions = pd.read_csv(cmw_submissions_sample_path, sep='\t')
df_comments = pd.read_csv(cmw_comments_sample_path, sep='\t')

# in case we want to discard deleted comments
#df_comments = df_comments.drop(df_comments[df_comments.body == '[deleted]'].index)

# turn to int?????? it's not even working
df_comments['parent_comment_id'].apply(lambda x: int(x) if math.isnan(x) is False else None)

# well, these are not working either
#df_comments['parent_comment_id'] = df_comments['parent_comment_id'].dropna()
#df_comments['parent_comment_id'] = df_comments['parent_comment_id'].astype(int)
#df_comments['parent_comment_id'] = df_comments['parent_comment_id'].astype('int', errors='ignore')

df_comments['parent_comment_id']
######################


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












# %% find unique participants in comment chain
print(len(delta_awarded_comments_id))
def find_unique_participants(parent_comment_id):
    parent_comment_ids = set()
    while math.isnan(parent_comment_id) == False:
            #parent_comment_id = df_comments[df_comments['id'] == parent_comment_id]['parent_comment_id'].squeeze()
            parent_comment = df_comments[df_comments['id'] == parent_comment_id]
            parent_comment_id = parent_comment['parent_comment_id'].squeeze()
            #number_of_replies += 1
            print(parent_comment_id)

            parent_comment_ids.add(parent_comment_id)
            print(parent_comment_ids)
    return parent_comment_ids

df_comments['unique_participants'] = df_comments[df_comments['id'].isin(delta_awarded_comments_id)]['parent_comment_id'].apply(lambda x: find_unique_participants(x))
df_comments['unique_participants'].dropna()
#########################



# %% count unique participants in comment chain
print(len(delta_awarded_comments_id))
def count_unique_participants(parent_comment_id):
    parent_comment_ids = set()
    while math.isnan(parent_comment_id) == False:
            #parent_comment_id = df_comments[df_comments['id'] == parent_comment_id]['parent_comment_id'].squeeze()
            parent_comment = df_comments[df_comments['id'] == parent_comment_id]
            parent_comment_id = parent_comment['parent_comment_id'].squeeze()
            #number_of_replies += 1
            #print(parent_comment_id)

            parent_comment_ids.add(parent_comment_id)
            #print(parent_comment_ids)
    return len(parent_comment_ids)

df_comments['unique_participants'] = df_comments[df_comments['id'].isin(delta_awarded_comments_id)]['parent_comment_id'].apply(lambda x: int(count_unique_participants(x)))
df_comments['unique_participants'] = df_comments['unique_participants'].dropna()
# still not integer datatype but an integer count nevertheless

#df_comments['unique_participants'].round().dropna()
df_comments['unique_participants'].convert_dtypes().dropna()
df_comments['unique_participants'] = df_comments['unique_participants'].convert_dtypes().dropna()
df_comments['unique_participants'].dropna()

#df_comments['unique_participants'].astype('Int64')
#df_comments['unique_participants'].astype(pd.Int64Dtype()).dropna()


#########################


# %% visualize it as a histogram
import matplotlib.pyplot as plt
import seaborn as sns

unique_participants = df_comments['unique_participants']#.dropna()

plt.figure(figsize=(10, 6))
sns.histplot(unique_participants, bins=30, kde=True)
plt.title('Distribution of Unique Participants in Comment Chains')
plt.xlabel('Number of Unique Participants')
plt.ylabel('Frequency')
plt.show()
#########################

# %% visualize it as a bar plot
# Calculating the counts of unique participant numbers
participant_counts = unique_participants.value_counts().sort_index()

# Plotting the bar plot
plt.figure(figsize=(14, 8))
sns.barplot(x=participant_counts.index, y=participant_counts.values, palette='viridis')
plt.title('Number of Comment Chains per Unique Participant Count')
plt.xlabel('Number of Unique Participants')
plt.ylabel('Number of Comment Chains')
#plt.xticks(rotation=90)  # Rotate x-axis labels if there are many
plt.show()

# cool but again: is there "double counting"? 
# i.e., when a comment chain contains multiple deltas, 
# is each of them counted as a separate "comment chain"?
# and thus, is the title misleading?
#########################




# %% failed attempt at: n% of delta threads where it's just m participants
#df_comments['unique_participants'] = df_comments[df_comments['id'].isin(delta_awarded_comments_id)]['parent_comment_id'].apply(lambda x: int(count_unique_participants(x)))
df_comments['unique_participants'] = df_comments['parent_comment_id'].apply(lambda x: count_unique_participants(x))
df_comments['unique_participants'] = df_comments['unique_participants'].convert_dtypes().dropna()

participants_delta = df_comments[df_comments['id'].isin(delta_awarded_comments_id)]['unique_participants']
participants_non_delta = df_comments[~df_comments['id'].isin(delta_awarded_comments_id)]['unique_participants']

delta_counts = participants_non_delta.value_counts().sort_index()
non_delta_counts = participants_delta.value_counts().sort_index()

# Plotting the bar plot
'''
plt.figure(figsize=(14, 8))
sns.barplot(x=[non_delta_counts.index, delta_counts.index], y=[non_delta_counts.values, delta_counts.values], palette='viridis')
plt.title('Number of Comment Chains per Unique Participant Count')
plt.xlabel('Number of Unique Participants')
plt.ylabel('Number of Comment Chains')
#plt.xticks(rotation=90)  # Rotate x-axis labels if there are many
plt.show()
'''
#########################

# %% not quite: n% of delta threads where it's just m participants
# Sample DataFrame setup (replace this with your actual DataFrame)
# df_comments = pd.read_csv('your_comments_data.csv')
# delta_awarded_comments_id = set([...])  # Replace with your actual set of delta awarded comment IDs

# Separate the DataFrame into two groups
awarded_comments = df_comments[df_comments['id'].isin(delta_awarded_comments_id)]
non_awarded_comments = df_comments[~df_comments['id'].isin(delta_awarded_comments_id)]

# Count the number of unique participants for both groups
awarded_counts = awarded_comments['unique_participants'].value_counts().sort_index()
non_awarded_counts = non_awarded_comments['unique_participants'].value_counts().sort_index()

# Create a DataFrame for plotting
count_df = pd.DataFrame({
    'Awarded': awarded_counts,
    'Non-Awarded': non_awarded_counts
}).fillna(0)  # Fill NaN values with 0 for counts that may not exist in one of the groups

# Plotting the grouped bar chart
count_df.plot(kind='bar', figsize=(14, 8), width=0.8)
plt.title('Comparison of Unique Participants in Comment Chains')
plt.xlabel('Number of Unique Participants')
plt.ylabel('Number of Comment Chains')
plt.legend(title='Comment Type')
plt.xticks(rotation=0)  # Rotate x-axis labels if needed
plt.show()
#########################


# %% not quite: n% of delta threads where it's just m participants
total_counts = df_comments['unique_participants'].value_counts().sort_index()

awarded_ratios = awarded_counts / total_counts
non_awarded_ratios = non_awarded_counts / total_counts

# Create a DataFrame for plotting
ratio_df = pd.DataFrame({
    'Awarded': awarded_ratios,
    'Non-Awarded': non_awarded_ratios
}).fillna(0)  # Fill NaN values with 0 for counts that may not exist in one of the groups

# Plotting the grouped bar chart
ratio_df.plot(kind='bar', figsize=(14, 8), width=0.8)
plt.title('Comparison of Unique Participants in Comment Chains')
plt.xlabel('Number of Unique Participants')
plt.ylabel('Number of Comment Chains')
plt.legend(title='Comment Type')
plt.xticks(rotation=0)  # Rotate x-axis labels if needed
plt.show()
#########################


# %% n% of delta threads where it's just m participants
# Calculating the counts of unique participant numbers
awarded_counts = awarded_comments['unique_participants'].value_counts().sort_index()
total_counts = df_comments['unique_participants'].value_counts().sort_index()
awarded_ratios = awarded_counts / sum(awarded_counts)
awarded_ratios = awarded_ratios.dropna()

# Plotting the bar plot
plt.figure(figsize=(14, 8))
sns.barplot(x=awarded_ratios.index, y=awarded_ratios.values, palette='viridis')
plt.title('Proportion of unique participants in delta threads')
plt.xlabel('Number of Unique Participants')
plt.ylabel('Proportion of Comment Chains')
#plt.xticks(rotation=90)  # Rotate x-axis labels if there are many
plt.show()
##########################