#### inspiration from: https://stackoverflow.com/questions/74528441/detect-passive-or-active-sentence-from-text

# %% imports
import spacy
from spacy.matcher import Matcher
import pandas as pd
import spacy_transformers


# %% paths
data_path = "../../data/work/samples/"
random_comments_sample_path = data_path + "random_sample_comments_sample_1.tsv"
civility_comments_sample_path = "../../data/work/" + "civility_annotation_set.tsv"
cmw_comments_sample_path = data_path + "cmw_comments_sample_1_delta_annotation.tsv"
eli5_comments_sample_path = data_path + "eli5_comments_sample_1.tsv"
#####################


# %% load cmw sample
df_random_comments = pd.read_csv(random_comments_sample_path, sep='\t')
df_civility = pd.read_csv(civility_comments_sample_path, sep='\t')
df_comments = pd.read_csv(cmw_comments_sample_path, sep='\t')
df_eli5_comments = pd.read_csv(eli5_comments_sample_path, sep='\t')
#####################


# %% passive_voice_function

from PassivePySrc import PassivePy
!python3 -m spacy download en_core_web_lg
passivepy = PassivePy.PassivePyAnalyzer(spacy_model = "en_core_web_lg")

def passive_voice_fnc(doc): 
    doc = nlp(doc)
    text = [str(sent.text) for sent in doc.sents]
    ret = None
    len_text = len(text)
    for sentence in text:
        doc = nlp(sentence)  # Process text with spaCy model
        matches = matcher(doc)  # Get matches
        #print("-"*40 + "\n" + sentence)
        sentence_match = None
        if len(matches) > 0:
            sentence_match = 0
            for match_id, start, end in matches:
                string_id = nlp.vocab.strings[match_id]
                span = doc[start:end]  # the matched span
                #print("\t{}: {}".format(string_id, span.text))
                if string_id == 'Active':
                    sentence_match += 0
                elif string_id == 'Passive':     
                    sentence_match += 1
            sentence_match = sentence_match/len(matches) 
        else:
            len_text -= 1           
        #else:
            #print("\tNo active or passive voice detected.")
            #active_passive_score.append(None)
        if sentence_match != None:
            if ret == None:
                ret = sentence_match
            else:
                ret += sentence_match    
    if ret == None:         
        return ret
    else: 
        return ret/len_text
    
def passive_voice_pckg(doc): 
    print(passivepy.match_text(doc, full_passive=True, truncated_passive=True))
    return passivepy.match_text(doc, full_passive=True, truncated_passive=True)


# %% passive_voice

import en_core_web_sm
nlp = en_core_web_sm.load()

# Load spaCy pipeline (model)
#nlp = spacy.load('en_core_web_trf')
# Create pattern to match passive voice use
passive_rules = [
    [{'DEP': 'nsubjpass'}, {'DEP': 'aux', 'OP': '*'}, {'DEP': 'auxpass'}, {'TAG': 'VBN'}],
    [{'DEP': 'nsubjpass'}, {'DEP': 'aux', 'OP': '*'}, {'DEP': 'auxpass'}, {'TAG': 'VBZ'}],
    [{'DEP': 'nsubjpass'}, {'DEP': 'aux', 'OP': '*'}, {'DEP': 'auxpass'}, {'TAG': 'RB'}, {'TAG': 'VBN'}],
]
# Create pattern to match active voice use
active_rules = [
    [{'DEP': 'nsubj'}, {'TAG': 'VBD', 'DEP': 'ROOT'}],
    [{'DEP': 'nsubj'}, {'TAG': 'VBP'}, {'TAG': 'VBG', 'OP': '!'}],
    [{'DEP': 'nsubj'}, {'DEP': 'aux', 'OP': '*'}, {'TAG': 'VB'}],
    [{'DEP': 'nsubj'}, {'DEP': 'aux', 'OP': '*'}, {'TAG': 'VBG'}],
    [{'DEP': 'nsubj'}, {'TAG': 'RB', 'OP': '*'}, {'TAG': 'VBG'}],
    [{'DEP': 'nsubj'}, {'TAG': 'RB', 'OP': '*'}, {'TAG': 'VBZ'}],
    [{'DEP': 'nsubj'}, {'TAG': 'RB', 'OP': '+'}, {'TAG': 'VBD'}],
]

matcher = Matcher(nlp.vocab)  # Init. the matcher with a vocab (note matcher vocab must share same vocab with docs)
matcher.add('Passive',  passive_rules)  # Add passive rules to matcher
matcher.add('Active', active_rules)  # Add active rules to matcher
active_passive_score = []


df_comments['passive score_2'] = df_comments['body'].apply(lambda x: passive_voice_fnc(str(x))) 

# for _, row in df_comments.iterrows():
#     doc = nlp(str(row['body']))
#     text = [str(sent.text) for sent in doc.sents]
#     for sentence in text:
#         doc = nlp(sentence)  # Process text with spaCy model
#         matches = matcher(doc)  # Get matches
#         #print("-"*40 + "\n" + sentence)
#         sentence_match = None
#         if len(matches) > 0:
#             sentence_match = 0
#             for match_id, start, end in matches:
#                 string_id = nlp.vocab.strings[match_id]
#                 span = doc[start:end]  # the matched span
#                 #print("\t{}: {}".format(string_id, span.text))
#                 if string_id == 'Active':
#                     sentence_match += 0
#                 elif string_id == 'Passive':     
#                     sentence_match += 1
#             sentence_match = sentence_match/len(matches)        
#         #else:
#             #print("\tNo active or passive voice detected.")
#             #active_passive_score.append(None)
#     if sentence_match == None:         
#         active_passive_score.append(sentence_match)
#     else: 
#         active_passive_score.append(sentence_match/len(text))       
#df_comments['passive score'] = active_passive_score

# %%
#df_comments['passive score_3'] = df_comments['body'].apply(lambda x: passive_voice_pckg(str(x))) 
for _, row in df_comments.iterrows():
    print(passivepy.match_corpus_level(str(row['body']), column_name='sentences', full_passive=True, truncated_passive=True).raw_passive_count)
# %% add to a column)
df_comments['body'] = df_comments['body'].apply(lambda x: str(x))
df_detected_s = passivepy.match_corpus_level(df_comments, column_name='body', full_passive=True, truncated_passive=True)

# %% save 
df_detected_s.to_csv('../../data/work/samples/cmw_comments_sample_1_delta_annotation.tsv', sep='\t')

# %% 
df_comments['passive score_2']
# %%
df_non_nan_comments = df_comments[df_comments['passive score_2'].notna()]
# %%
# %%
from scipy.stats import norm
import numpy as np
import matplotlib.pyplot as plt

passive_voice_scores1 = df_detected_s[df_detected_s['delta'] == True]['passive_percentages']
passive_voice_scores2 = df_detected_s[df_detected_s['delta'] == False]['passive_percentages']
# Combine readability scores for easier curve fitting and plotting
all_scores = passive_voice_scores1 + passive_voice_scores2

# Plot overlayed histograms
plt.figure(figsize=(10, 6))

counts1, bins1, _ = plt.hist(passive_voice_scores1, bins=100, alpha=0.5, color='blue', edgecolor='black', label='Delta awarded comments', density=True)
counts2, bins2, _ = plt.hist(passive_voice_scores2, bins=100, alpha=0.5, color='red', edgecolor='black', label='Non-delta awarded comments', density=True)


plt.title('Histogram of Passive Voice Usage')
plt.xlabel('Passive voice percentage')
plt.ylabel('Density')
plt.legend()

# Show the plot
plt.show()
# %%
print('Delta Passive voice score: ', passive_voice_scores1.mean())
print('Non Delta Passive voice score: ', passive_voice_scores2.mean())


# %%
from datetime import datetime
power_users_comments_path = data_path + 'power_user_cmv_comments.tsv'
power_users = pd.read_csv(power_users_comments_path , sep='\t')
power_users = power_users[power_users['created_utc'].notna()]
power_users['created_utc'] = power_users['created_utc'].apply(lambda x: (datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S')))
power_users = power_users.sort_values(by='created_utc')
power_users['body'] = power_users['body'].apply(lambda x: str(x))
power_users = passivepy.match_corpus_level(power_users, column_name='body', full_passive=True, truncated_passive=True)
power_users.to_csv('../../data/work/samples/power_user_cmv_comments.tsv', sep='\t')



# %%
