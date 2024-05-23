import pandas as pd
import spacy
from bertopic import BERTopic

# Read the text file into a DataFrame
file_path = 'title_and_scores1.txt'
df = pd.read_csv(file_path, delimiter='\t', names=['ID', 'Title', 'Score'])

# Display the first few rows of the dataframe to inspect
print(df.head())

# Ensure scores are numeric
df['Score'] = pd.to_numeric(df['Score'], errors='coerce')
df.dropna(subset=['Score'], inplace=True)

# Aggregate scores by title
title_scores = df.groupby('Title')['Score'].sum().reset_index()

# Display the aggregated scores
print(title_scores.head())

# Load spaCy model
nlp = spacy.load("en_core_web_trf")

# Read the text file into a DataFrame
file_path = 'title_and_scores1.txt'
df = pd.read_csv(file_path, delimiter='\t', names=['ID', 'Title', 'Score'])

# Ensure scores are numeric
df['Score'] = pd.to_numeric(df['Score'], errors='coerce')
df.dropna(subset=['Score'], inplace=True)

# Aggregate scores by title
title_scores = df.groupby('Title')['Score'].sum().reset_index()

# Function to preprocess titles using spaCy
def preprocess(text):
    doc = nlp(text.lower())
    tokens = [token.lemma_ for token in doc if token.is_alpha and not token.is_stop]
    return ' '.join(tokens)

title_scores['Processed_Title'] = title_scores['Title'].apply(preprocess)

# Display the preprocessed titles
print(title_scores.head())

# Create a BERTopic model
topic_model = BERTopic(language="english")

# Fit the model on the processed titles
topics, probabilities = topic_model.fit_transform(title_scores['Processed_Title'])

# Assign topics to each title
title_scores['Topic'] = topics

# Calculate total score for each topic
topic_scores = title_scores.groupby('Topic')['Score'].sum().reset_index()

# Write topics with their scores to a file
with open('cmv_sample_top_words.txt', 'w') as file:
    for topic in topic_scores['Topic']:
        score = topic_scores[topic_scores['Topic'] == topic]['Score'].values[0]
        topic_info = topic_model.get_topic(topic)
        
        file.write(f"Topic {topic}: Score = {score}\n")
        for word, weight in topic_info:
            file.write(f"{word}: {weight}\n")
        file.write('\n')
