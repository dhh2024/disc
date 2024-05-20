from hereutil import here, add_to_sys_path
from src.common_basis import *
import pandas as pd
import os
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
delta_comments_subsample = real_delta_comments.sample(3000).copy()
delta_comments_subsample["delta"] = 1
#%%
random_sample_comments = pd.read_csv(here("data/work/samples/random_sample_comments_sample_1.tsv"), sep="\t")
random_sample_comments["delta"] = 0

#%%

data_for_classifier = pd.concat([delta_comments_subsample, random_sample_comments])

#%%

def classifier(data):
    X = data['body']
    y = data['delta']


    # modify the data to make null cells into empty strings
    X = X.fillna("")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)



    def custom_tokenizer(text):
        text.lower()
        return text.split(" ")

    pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(ngram_range=(1,2), tokenizer=custom_tokenizer)),
        ('rf', RandomForestClassifier())
    ])

    pipeline.fit(X_train, y_train)

    # Get feature names from TfidfVectorizer
    feature_names = pipeline.named_steps['tfidf'].get_feature_names_out()



    # Get feature importances by class
    importances_by_class = {}
    for class_index, class_label in enumerate(pipeline.named_steps['rf'].classes_):
        # Binary classification: one class vs rest
        y_binary = (y_train == class_label)
        rf_binary = RandomForestClassifier()
        rf_binary.fit(pipeline.named_steps['tfidf'].transform(X_train), y_binary)
        importances_by_class[class_label] = dict(
            sorted(zip(feature_names, rf_binary.feature_importances_), key=lambda x: x[1], reverse=True))

    # Print feature importances by class
    for class_label, importances in importances_by_class.items():
        print(f"\nClass: {class_label}")
        for feature_name, importance in list(importances.items())[:25]:
            print(f"{feature_name}: {importance}")

    def custom_predict(texts, pipeline):
        predictions = pipeline.predict(texts)
        # Update predictions based on the tokenized length
        return predictions

    print("predictions")
    # Evaluation on test set
    predictions = pipeline.predict(X_test)
    print(classification_report(y_test, predictions))

    print(X_test[y_test != predictions])
    dict_of_misclassified = {X_test[y_test != predictions].index[i]: X_test[y_test != predictions].iloc[i] for i in range(len(X_test[y_test != predictions]))}
    return dict_of_misclassified, importances_by_class

#%%
x, y = classifier(data_for_classifier)