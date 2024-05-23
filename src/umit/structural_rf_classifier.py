from hereutil import here, add_to_sys_path
from src.common_basis import *
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import numpy as np
import pandas as pd

# %%
add_to_sys_path(here())
eng, con = get_db_connection()

#%%

"""RANDOM FOREST CLASSIFIER FOR THE STRUCTURAL FEATURES IN THE POSTER"""

random_sample = pd.read_csv(here("data/work/samples/random_4000_sample.tsv"), sep="\t")
data_driven_sample = pd.read_csv(here("data/work/samples/data_driven_4000_sample.tsv"), sep="\t")

# drop an unnamed column
data_driven_sample = data_driven_sample.drop("Unnamed: 0", axis=1)

data_driven_sample["civil"] = 1
random_sample["civil"] = 0

# merge the dataframes
civil_and_random = pd.concat([data_driven_sample, random_sample])


# from the civil_and_random df, drop the body, subreddit_id, subreddit, permalink, link_id, parent_comment_id, created_utc, author_id, author columns
civil_and_random = civil_and_random.drop(["body", "subreddit_id", "subreddit", "permalink", "link_id", "parent_comment_id", "created_utc", "author_id", "author",
                                          "all_passives", "full_passive_matches", "truncated_passive_matches", "height", "id" ], axis=1)
civil_and_random = civil_and_random.drop("Unnamed: 0", axis=1)


X = civil_and_random.drop('civil', axis=1)
y = civil_and_random['civil']

feature_list = list(X.columns)
features = np.array(feature_list)

# split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=3)

rf = RandomForestClassifier(n_estimators=1000, random_state=42)
rf.fit(X_train, y_train)

predictions = rf.predict(X_test)

# Get numerical feature importances
importances = list(rf.feature_importances_)
# List of tuples with variable and importance
feature_importances = [(feature, round(importance, 2)) for feature, importance in zip(feature_list, importances)]
# Sort the feature importances by most important first
feature_importances = sorted(feature_importances, key = lambda x: x[1], reverse = True)
# Print out the feature and importances
[print('Variable: {:20} Importance: {}'.format(*pair)) for pair in feature_importances]

#show classification report
print(classification_report(y_test, predictions))

