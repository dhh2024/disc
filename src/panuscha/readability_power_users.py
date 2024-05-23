# %% imports
import pandas as pd
import math
from datetime import datetime
import matplotlib.pyplot as plt
import nltk
nltk.download('punkt')

# %% paths
data_path = "../../data/work/samples/"
cmw_comments_sample_path = data_path + "cmw_comments_sample_1_delta_annotation.tsv"
cmw_submissions_sample_path = data_path + "cmw_submissions_sample_1.tsv"
cwm_delta_thread_sample = data_path + "cmw_comments_sample_1_deltas_thread.tsv"
power_users_comments_path = data_path + 'power_user_cmv_comments.tsv' #'random_sample_comments_sample_1.tsv'

# %% load power_users
power_users = pd.read_csv(power_users_comments_path , sep='\t')
power_users = power_users[power_users['created_utc'].notna()]
power_users['created_utc'] = power_users['created_utc'].apply(lambda x: (datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S')))
power_users = power_users.sort_values(by='created_utc')

# %% readability score
import textstat
readability_comments_score = []
comments_tokens = []

for _, row in power_users.iterrows():
    try:
        if len(str(row['body']).split()) > 100:
            score = textstat.flesch_reading_ease(str(row['body']))
            if score > 0:
                readability_comments_score.append(score)
                comments_tokens.append(len(str(row['body']).split()))
            else:
                readability_comments_score.append(None)    
        else:
                readability_comments_score.append(None)               
    except:
        readability_comments_score.append(None)             
power_users['readability_score'] = readability_comments_score
power_users = power_users[power_users['readability_score'].notna()]

# %% save 
power_users.to_csv('../../data/work/samples/power_user_cmv_comments_readability.tsv', sep='\t')


# %% plot 
users = power_users.author.unique()

for user in users[:1]:
    plt.plot(power_users[power_users['author'] == user]['created_utc'], power_users[power_users['author'] == user]['readability_score'], marker='o', label=user)

plt.xlabel('Time')
plt.ylabel('Readability Score')
plt.title('Readability Scores Over Time')
plt.legend()
plt.grid(True)
plt.show()


# %%
power_users['rolling_avg' ] = power_users.readability_score.rolling(200).mean() 
users = power_users.author.unique()

for user in users[:1]:
    plt.plot(power_users[power_users['author'] == user]['created_utc'], power_users[power_users['author'] == user]['rolling_avg'], linestyle="solid" , label=user)

plt.xlabel('Time')
plt.ylabel('Readability Score')
plt.title('Readability Scores Over Time')
plt.legend()
plt.grid(True)
plt.show()



# %%
users = power_users.author.unique()
rolling = 500

for user in users:
    power_users.loc[power_users['author'] == user, 'rolling_avg'] = power_users[power_users['author'] == user].readability_score.rolling(rolling).mean() 

power_users['rolling_avg_all'] = power_users.readability_score.rolling(10000).mean() 


for user in users:
    user_data = power_users[power_users['author'] == user].sort_values(by='created_utc')
    plt.plot(user_data['created_utc'], user_data['rolling_avg'], label=user, alpha = 0.5, linestyle="solid")




plt.plot(power_users['created_utc'], power_users['rolling_avg_all'], label=user, alpha = 1, color = 'black', linestyle="solid")
plt.xlabel('Time')
plt.ylabel('Moving Avg Readability Score')
plt.title('Readability Scores of Power Users Over Time')
plt.show()


# %%
power_users.tail()
# %%
