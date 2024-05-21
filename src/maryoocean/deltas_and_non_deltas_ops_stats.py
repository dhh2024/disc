# %% [markdown]
# In this file I [Marina] worked with `cmw_submissions_sample_1_deltas.tsv` and `cmw_submissions_sample_1_non-deltas.tsv` available on [GDrive](https://drive.google.com/drive/folders/1MnayT571RYFxySvzdERbpf-sJGXJhIE5?usp=share_link).
#
# The goal was to see if there are any significant differences in the posts themselves that might correlate with deltas (not)-being awarded. Hypotheses were be:
# 1. Posts with no deltas are either too short or too long for other users to engage in the discussion, the length marks the OP's willingness to elaborate on their opinion just enough for others to change it. -- barely?  however, posts with no deltas seem to be more incosistent in terms of their length in sentences
#
# 2. Posts with deltas have some markers that signal that the OP's is open to change their view, that they do not post their opinion to just state it but are trying to challenge themselves. -- assessed with close-reading as of now. Could be that phrases like "change my view", "please help me to change my view", "CMV" at the end of the post appear more often in the submissions with delta than without. Needs to be looked into (maybe try to count entries).
#
# Next step: we can list some linguistic cues to try to assess their
# representation in the subsets via close-reading or computationally.

# %%
import pandas as pd
import stanza
import re
import matplotlib.pyplot as plt
import numpy as np
import random


# %%
def open_df(filename):
    df = pd.read_csv(filename, sep='\t', header=0)
    return df


# %%
def del_footnote(texts):
    texts_clear = []
    for text in texts:
        try:
            piece_to_del = re.findall(
                r'_____\n\n&gt;.*?\[read through our rules\]\(.*?\).*?downvotes don\'t change views\]\(.*?\).*?popular topics wiki\]\(.*?\).*?\[message us\]\(.*?\)\*\*\*\. \*Happy CMVing!\*',
                text,
                re.DOTALL)[0]
            text = text.replace(piece_to_del, '')
            # print(text)
            texts_clear.append(text)
        except IndexError:
            texts_clear.append(text)
    return texts_clear


# %%
def count_sent(df):
    for i in list(df):
        if i == 'title':
            titles = df[i].tolist()
        elif i == 'selftext':
            texts = df[i].tolist()
        elif i == 'id':
            ids = df[i].tolist()
    texts_clear = del_footnote(texts)
    text_length = []
    nlp = stanza.Pipeline(lang='en', processors='tokenize')
    for text in texts_clear:
        doc = nlp(text)
        num_of_sent = 0
        for i, sentence in enumerate(doc.sentences):
            num_of_sent += 1
        text_length.append(num_of_sent)
    print(text_length)
    return titles, texts_clear, ids, text_length


# %%
def new_df(titles, texts, ids, text_length, filename):
    df_new = pd.DataFrame({'id': ids,
                           'title': titles,
                           'selftext': texts,
                           'num_sentences': text_length})
    df_new = df_new.astype(
        {"id": str, "title": str, "selftext": str, "num_sentences": int})
    df_new.to_csv(filename, sep="\t", index=False)
    return df_new


# %%
no_deltas = open_df("cmw_submissions_sample_1_non-deltas.tsv")
noD_titles, noD_texts, noD_ids, noD_text_length = count_sent(no_deltas)
noD_df = new_df(noD_titles, noD_texts, noD_ids, noD_text_length, "noD_df.tsv")

# %%
noD_df

# %%
print("NO DELTA POSTS")
print(f"Median number of sentences: {noD_df['num_sentences'].median()}")
print(f"Average number of sentences: {noD_df['num_sentences'].mean()}")
print(f"Minimum number of sentences: {noD_df['num_sentences'].min()}")
print(f"Maximum number of sentences: {noD_df['num_sentences'].max()}")

# %%
deltas = open_df("cmw_submissions_sample_1_deltas.tsv")
D_titles, D_texts, D_ids, D_text_length = count_sent(deltas)
D_df = new_df(D_titles, D_texts, D_ids, D_text_length, "D_df.tsv")

# %%
print("DELTA POSTS")
print(f"Median number of sentences: {D_df['num_sentences'].median()}")
print(f"Average number of sentences: {D_df['num_sentences'].mean()}")
print(f"Minimum number of sentences: {D_df['num_sentences'].min()}")
print(f"Maximum number of sentences: {D_df['num_sentences'].max()}")

# %%
noD_df = open_df("noD_df.tsv")
D_df = open_df("D_df.tsv")


# %%
def lexicostat(df):
    for i in list(df):
        if i == 'selftext':
            texts = df[i].tolist()
        elif i == 'id':
            ids = df[i].tolist()
    texts_dict = {ids[i]: texts[i] for i in range(len(ids))}
    return texts_dict


# %%
noD_texts_dict = lexicostat(noD_df)
print(noD_texts_dict)

# %%
D_texts_dict = lexicostat(D_df)
print(D_texts_dict)


# %%
def tokenization(dic):
    annotated_d_texts = {}
    nlp = stanza.Pipeline(lang='en', processors='tokenize')
    for id_number, text in dic.items():
        doc = nlp(text)
        tokenized = []
        for sentence in doc.sentences:
            for token in sentence.tokens:
                tokenized.append(token.text)
                tokenized_text = " ".join(tokenized)
        annotated_d_texts[id_number] = tokenized_text
    return annotated_d_texts


# %%
annotated_D_texts = tokenization(D_texts_dict)

# %%
print(len(annotated_D_texts.values()))
print(len(annotated_noD_texts.values()))

# %%
requests = ['change my view',
           'please',
           'cmv',
           'my view']
politeness = ['thank you',
             'thanks']


# %%
def count_requests(annotated, phrases):
    counted = {}
    for phrase in phrases:
        counted[phrase] = 0
        for text in random.sample(list(annotated.values()), 200):
            text = text.lower()
            i = 0
            i += text.count(phrase)
            if i != 0:
                counted[phrase] += i
                #print(text)
    print(counted)


# %%
count_requests(annotated_D_texts, requests)

# %%
count_requests(annotated_D_texts, politeness)

# %%
annotated_noD_texts = tokenization(noD_texts_dict)

# %%
count_requests(annotated_noD_texts, requests)

# %%
count_requests(annotated_noD_texts, politeness)

# %% [markdown]
# We def need to preprocess texts better to be able to measure "open-mindedness" by usage of phrases like "change my view" and appreciation of other users' attempts.
