# Analysis on power users.
# Run this file with argument
#   l for comment lenghts
#   v for upvote information
#   q for quote information
#


# %%
from hereutil import here, add_to_sys_path
add_to_sys_path(here())
import sys
from src.common_basis import *
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np  

# %%
try:
    power_user_cmv_comments = pd.read_csv(here('src/heljae/data/power_user_cmv_comments.tsv'), sep='\t')
    power_user_aita_comments = pd.read_csv(here('src/heljae/data/power_user_aita_comments.tsv'), sep='\t')
    power_user_uo_comments = pd.read_csv(here('src/heljae/data/power_user_unpopularopinion_comments.tsv'), sep='\t')
    power_user_eli5_comments = pd.read_csv(here('src/heljae/data/power_user_eli_comments.tsv'), sep='\t')
except FileNotFoundError:
    power_user_cmv_comments = pd.read_parquet('dhh24/disc/parquet/power_user_cmv_all_comments.parquet', filesystem=get_s3fs(),engine="pyarrow")
    power_user_aita_comments = pd.read_parquet('dhh24/disc/parquet/power_user_aita_comments.parquet', filesystem=get_s3fs(),engine="pyarrow")
    power_user_eli5_comments = pd.read_parquet('dhh24/disc/parquet/power_user_unpopularopinion_comments.parquet', filesystem=get_s3fs(),engine="pyarrow")
    power_user_uo_comments = pd.read_parquet('dhh24/disc/parquet/power_user_eli5_comments.parquet', filesystem=get_s3fs(),engine="pyarrow")


power_user_cmv_comments = power_user_cmv_comments[~power_user_cmv_comments.body.str.contains(r'your comment has been removed|\[deleted\]',na=True)]
power_user_aita_comments = power_user_aita_comments[~power_user_aita_comments.body.str.contains(r'your comment has been removed|\[deleted\]',na=True)]
power_user_uo_comments = power_user_uo_comments[~power_user_uo_comments.body.str.contains(r'your comment has been removed|\[deleted\]',na=True)]
power_user_eli5_comments = power_user_eli5_comments[~power_user_eli5_comments.body.str.contains(r'your comment has been removed|\[deleted\]',na=True)]

non_power_aita_comments = pd.read_csv(here("data/work/samples/aita_comments_sample_1.tsv"), sep='\t')
non_power_aita_comments = non_power_aita_comments[~non_power_aita_comments.body.str.contains(r'your comment has been removed|\[deleted\]',na=True)]

non_power_cmv_comments = pd.read_csv(here("data/work/samples/cmw_comments_sample_1.tsv"), sep='\t')
non_power_cmv_comments = non_power_cmv_comments[~non_power_cmv_comments.body.str.contains(r'your comment has been removed|\[deleted\]',na=True)]

non_power_eli5_comments = pd.read_csv(here("data/work/samples/eli5_comments_sample_1.tsv"), sep='\t')
non_power_eli5_comments = non_power_eli5_comments[~non_power_eli5_comments.body.str.contains(r'your comment has been removed|\[deleted\]',na=True)]

non_power_uo_comments = pd.read_csv(here("data/work/samples/unpopularopinion_comments_sample_1.tsv"), sep='\t')
non_power_uo_comments = non_power_uo_comments[~non_power_uo_comments.body.str.contains(r'your comment has been removed|\[deleted\]',na=True)]
    
# %%



def draw_histogram(s1, s2, s1_name, s2_name, title, xlab, xticks, color):
    fig, ax = plt.subplots(1,2)
    
    fig.supxlabel(xlab)
    fig.supylabel('frequency')
    plt.suptitle(title, fontsize='x-large')
    plt.tight_layout()

    ax[0].hist(np.array(s1.array), bins=30,color=color)
    ax[0].set_xticks(xticks)
    ax[0].axvline(s1.array.mean(), color='k', linestyle='dashed', linewidth=1, label='mean')
    ax[0].text(s1.array.mean()+5,5,round(s1.array.mean(),1))
    ax[0].set_title(s1_name)
    
    ax[1].hist(np.array(s2.array), bins=30, color=color)
    ax[1].set_xticks(xticks)
    ax[1].axvline(s2.array.mean(), color='k', linestyle='dashed', linewidth=1, label='mean')
    ax[1].text(s2.array.mean()+5,5,round(s2.array.mean(),1))
    ax[1].set_title(s2_name)
    plt.show()



#%% 
def length_histograms():
    # comment lengths
    # %%
    ticks = list(range(0,1875,250))

    cmv_comments_lens = power_user_cmv_comments['body'].map(lambda x: len(str(x).split())) 
    np_cmv_lens = non_power_cmv_comments['body'].map(lambda x: len(str(x).split()))
    draw_histogram(
        cmv_comments_lens,
        np_cmv_lens,
        "Power user comments",
        "All comments",
        "Power user comment length in r/ChangeMyView",
        "number of words",
        ticks,
        "turquoise")
    plt.show()

    # %%
    aita_comments_lens = power_user_aita_comments['body'].map(lambda x: len(str(x).split()))
    np_aita_lens = non_power_aita_comments['body'].map(lambda x: len(str(x).split()))
    draw_histogram(
        aita_comments_lens,
        np_aita_lens,
        "Power user comments",
        "All comments",
        "Power user comment length in r/AmItheA**hole",
        "number of words",
        ticks,
        "coral")
    plt.show()

    # %%
    eli5_comments_lens = power_user_eli5_comments['body'].map(lambda x: len(str(x).split()))
    np_eli5_lens = non_power_eli5_comments['body'].map(lambda x: len(str(x).split()))
    draw_histogram(
        eli5_comments_lens,
        np_eli5_lens,
        "Power user comments",
        "All comments",
        "Power user comment length in r/ExplainLikeIm5",
        "number of words", 
        ticks,
        "y")
    plt.show()

    # %%
    uo_comments_lens = power_user_uo_comments['body'].map(lambda x: len(str(x).split()))
    np_uo_lens = non_power_uo_comments['body'].map(lambda x: len(str(x).split()))
    draw_histogram(
        uo_comments_lens,
        np_uo_lens,
        "Power user comments",
        "All comments",
        "Power user comment length in r/UnpopularOpinion",
        'number of words',
        ticks,
        'plum')
    plt.show()


# %%
def vote_averages():
    # upvotes
    #%%
    cmv_upvotes = power_user_cmv_comments['score']
    aita_upvotes = power_user_aita_comments['score']
    eli5_upvotes = power_user_eli5_comments['score']
    uo_upvotes = power_user_uo_comments['score']

    np_cmv_upv = non_power_cmv_comments['score']
    np_aita_upv = non_power_aita_comments['score']
    np_eli5_upv = non_power_eli5_comments['score']
    np_uo_upv = non_power_uo_comments['score']

    df = pd.DataFrame([[
        cmv_upvotes.mean(),
        aita_upvotes.mean(),
        eli5_upvotes.mean(),
        uo_upvotes.mean()],
        [np_cmv_upv.mean(),
         np_aita_upv.mean(),
         np_eli5_upv.mean(),
         np_uo_upv.mean()]
        ], columns=[
        "cmv",'aita','eli5','uo'
    ], index=['Power user avg vote ratio', 'All avg vote ratio']).transpose()

    df['Power user avg vote ratio'] = df['Power user avg vote ratio'].apply(lambda x: round(x,1))
    df['All avg vote ratio'] = df['All avg vote ratio'].apply(lambda x: round(x,1))

    print(df)
    df



# %%
def comments_with_quotes():
    # %%
    p = r"\s*>.[\sA-Za-z\d\"\']+|\s*\&gt;"

    # power users
    cmv_quotes = power_user_cmv_comments.body.str.contains(p,regex=True,na=False)
    cmv_quotes = power_user_cmv_comments[cmv_quotes]
    cmv_quote_ratio = round(cmv_quotes.shape[0]/power_user_cmv_comments.shape[0]*100,2)

    aita_quotes = power_user_aita_comments.body.str.contains(p,regex=True,na=False)
    aita_quotes = power_user_aita_comments[aita_quotes]
    aita_quote_ratio = round(aita_quotes.shape[0]/power_user_aita_comments.shape[0]*100,2)

    eli5_quotes = power_user_eli5_comments.body.str.contains(p,regex=True,na=False)
    eli5_quotes = power_user_eli5_comments[eli5_quotes]
    eli5_quote_ratio = round(eli5_quotes.shape[0]/power_user_eli5_comments.shape[0]*100,2)

    uo_quotes = power_user_uo_comments.body.str.contains(p,regex=True,na=False)
    uo_quotes = power_user_uo_comments[uo_quotes]
    uo_quote_ratio = round(uo_quotes.shape[0]/power_user_uo_comments.shape[0]*100,2)


    # non power users
    aita_non_power_quotes = non_power_aita_comments.body.str.contains(p,regex=True,na=False)
    aita_non_power_quotes = non_power_aita_comments[aita_non_power_quotes]
    np_aita_quote_ratio = round(aita_non_power_quotes.shape[0]/non_power_aita_comments.shape[0]*100,2)

    cmv_non_power_quotes = non_power_cmv_comments.body.str.contains(p,regex=True,na=False)
    cmv_non_power_quotes = non_power_cmv_comments[cmv_non_power_quotes]
    np_cmv_quote_ratio = round(cmv_non_power_quotes.shape[0]/non_power_cmv_comments.shape[0]*100,2)

    eli5_non_power_quotes = non_power_eli5_comments.body.str.contains(p,regex=True,na=False)
    eli5_non_power_quotes = non_power_eli5_comments[eli5_non_power_quotes]
    np_eli5_quote_ratio = round(eli5_non_power_quotes.shape[0]/non_power_eli5_comments.shape[0]*100,2)

    uo_non_power_quotes = non_power_uo_comments.body.str.contains(p,regex=True,na=False)
    uo_non_power_quotes = non_power_uo_comments[uo_non_power_quotes]
    np_uo_quote_ratio = round(uo_non_power_quotes.shape[0]/non_power_uo_comments.shape[0]*100,2)

    # tie it together
    df = pd.DataFrame(
        [
            [cmv_quote_ratio,aita_quote_ratio,eli5_quote_ratio,uo_quote_ratio],
            [np_cmv_quote_ratio,np_aita_quote_ratio,np_eli5_quote_ratio,np_uo_quote_ratio]
        ],
        index=['% of power user comments', '% of non-power user comments'],
        columns=['CMV','AITA','ELI5','unpopular opinion']        
    )
    df = df.transpose()
    print(df)
    df


# %%
if __name__ == "__main__":
    a = sys.argv[-1]

    if a == 'l':
        length_histograms()
    if a == 'v':
        vote_averages()
    if a == 'q':
        comments_with_quotes()
    else:
        pass

# %%
