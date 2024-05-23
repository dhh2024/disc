# %% imports
%load_ext autoreload
%autoreload 2
from hereutil import here, add_to_sys_path
add_to_sys_path(here()) # noqa
from src.common_basis import *
import pandas as pd

# %% paths
eng, con = get_db_connection()
tables = pd.read_sql('SHOW TABLES;', con)


# %%
for _, row in tables.iterrows():
    print(row.Tables_in_disc)
#table_1 = pd.read_sql_table('best_of_reddit_comments_a', con)


# %%
best_of_reddit = pd.read_sql_table('best_of_reddit_comments_b', con)

# %%
print(best_of_reddit.columns)

# %%
data_driven_civil_discourse = pd.read_sql_table("data_driven_civil_discourse_subset_a", con)
print(data_driven_civil_discourse.columns)

# %%
print(len(data_driven_civil_discourse.index))

# %%
from PassivePySrc import PassivePy
!python3 -m spacy download en_core_web_lg
passivepy = PassivePy.PassivePyAnalyzer(spacy_model = "en_core_web_lg")


# %%
df_civil_discourse_passive = passivepy.match_corpus_level(data_driven_civil_discourse.iloc[:1000], column_name='body', full_passive=True, truncated_passive=True)


# %%
df_civil_discourse_passive.head()

# %%
