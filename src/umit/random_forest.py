from hereutil import here, add_to_sys_path
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
eng, con = get_db_connection()

#%%

real_delta_comments = pd.read_sql("SELECT * FROM cmw_comments_a WHERE id IN (SELECT parent_comment_id FROM cmw_delta_comments_a);", con)
delta_comments_subsample = real_delta_comments.sample(4000).copy()
delta_comments_subsample = delta_comments_subsample[~delta_comments_subsample['body'].str.contains("removed|deleted")]
delta_comments_subsample["delta"] = 1

#%%

random_sample_comments = pd.read_csv(here("data/work/samples/random_sample_comments_sample_1.tsv"), sep="\t")
random_sample_comments = random_sample_comments[~random_sample_comments['body'].str.contains("removed|deleted")]
random_sample_comments["delta"] = 0

#%%

data_to_classify = pd.concat([delta_comments_subsample, random_sample_comments])

#%%



X = data_to_classify['body']
y = data_to_classify['delta']


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
