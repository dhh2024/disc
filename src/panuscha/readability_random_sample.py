# %% imports
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import nltk
nltk.download('punkt')

%load_ext autoreload
%autoreload 2
from hereutil import here, add_to_sys_path
add_to_sys_path(here()) # noqa
from src.common_basis import *


# %% paths
eng, con = get_db_connection()
tables = pd.read_sql('SHOW TABLES;', con)

# %% show tables
for _, row in tables.iterrows():
    print(row.Tables_in_disc)



# %% random sample
random_sample = pd.read_sql_table('cmw_delta_comments_a', con)
random_sample = random_sample[random_sample['created_utc'].notna()]
random_sample['created_utc'] = random_sample['created_utc'].apply(lambda x: (datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S')))
random_sample = random_sample.sort_values(by='created_utc')


# %% readability 
random_sample.head()

# %% readability 
import textstat
def readability(body):
    try:
        if len(body.split()) > 100:
            score = textstat.flesch_reading_ease(body)
            if score > 0:
                return score
            else:
                return None    
        else:
            return None               
    except:
        return None     

# %% 
       
random_sample['readability_score'] = random_sample['body'].apply(lambda x: readability(str(x)))
random_sample = random_sample[random_sample['readability_score'].notna()]


# %% plot
random_sample['readability_score']

# %% plot
random_sample['rolling_avg_all'] = random_sample.readability_score.rolling(1000).mean() 

plt.plot(random_sample['created_utc'], random_sample['rolling_avg_all'], alpha = 1, color = 'black', linestyle="solid")
plt.xlabel('Time')
plt.ylabel('Moving Avg Readability Score')
plt.title('Readability Scores of Random Sample Over Time')
plt.show()




# %%
