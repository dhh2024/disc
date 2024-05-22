# umit's random forest code modified for power user data
# %%
from hereutil import here, add_to_sys_path
add_to_sys_path(here())
from src.common_basis import *
import pandas as pd
import re
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.pipeline import Pipeline

# %%
add_to_sys_path(here())
DATA_DIR = here("data")
#eng, con = get_db_connection()

#%%

power_user_delta_comments = pd.read_parquet('dhh24/disc/parquet/power_user_cmv_all_comments.parquet', filesystem=get_s3fs(),engine="pyarrow")
power_users = power_user_delta_comments['author'].unique()
delta_comments_subsample = power_user_delta_comments.sample(4000).copy()
delta_comments_subsample = delta_comments_subsample[~delta_comments_subsample['body'].str.contains("removed|deleted", na=True)] # if body is NaN it doesn't exist, so it's 'the same' as deleted
delta_comments_subsample["power_user"] = 1

#%%
random_sample_comments = pd.read_csv("/home/raisaneh/k/dhh/disc/data/work/samples/cmw_comments_sample_1.tsv", sep="\t")
random_sample_comments = random_sample_comments[~random_sample_comments['body'].str.contains("removed|deleted", na=True)]
random_sample_comments = random_sample_comments[~random_sample_comments['author'].isin(power_users)]
random_sample_comments = random_sample_comments.sample(4000).copy()
random_sample_comments["power_user"] = 0

#%%

data_to_classify = pd.concat([delta_comments_subsample, random_sample_comments])

#%%



X = data_to_classify['body']
y = data_to_classify['power_user']


def custom_tokenizer(text):
    text.lower()
    return text.split(" ")

def custom_tokenizer_regex(text):
    text.lower()
    tokens = re.findall(r'\w+|&\w+;|[^\w\s]', text, re.UNICODE)
    return tokens

pipeline = Pipeline([
    ('tfidf', TfidfVectorizer(ngram_range=(1,2) )),
    ('rf', RandomForestClassifier())
])

# how can i see what the tokenizer is doing?
tokenized = pipeline.named_steps['tfidf'].fit_transform(X)
# show tokenized as text
tokenized_text = pipeline.named_steps['tfidf'].inverse_transform(tokenized)
#print(tokenized_text[:110])
pipeline.fit(X, y)

# Get feature names from TfidfVectorizer
feature_names = pipeline.named_steps['tfidf'].get_feature_names_out()

# Get feature importances by class
importances_by_class = {}
for class_index, class_label in enumerate(pipeline.named_steps['rf'].classes_):
    # Binary classification: one class vs rest
    y_binary = (y == class_label)
    rf_binary = RandomForestClassifier(oob_score=classification_report)
    rf_binary.fit(pipeline.named_steps['tfidf'].transform(X), y_binary)
    importances_by_class[class_label] = dict(
        sorted(zip(feature_names, rf_binary.feature_importances_), key=lambda x: x[1], reverse=True))

# Print feature importances by class
for class_label, importances in importances_by_class.items():
    print(f"\nClass: {class_label}")
    for feature_name, importance in list(importances.items())[:25]:
        print(f"{feature_name}: {importance}")

# report classification score on the oob set
print(f"oob score: {rf_binary.oob_score_}")
