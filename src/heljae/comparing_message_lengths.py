# compare comment and submission length between cmv and random

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

cmv_comments = pd.read_csv("../../data/work/samples/cmw_comments_sample_1.tsv", sep="\t")
cmv_comments = cmv_comments[cmv_comments.body != "[deleted]"]

cmv_ops = pd.read_csv("../../data/work/samples/cmw_submissions_sample_1.tsv", sep="\t")
cmv_ops = cmv_ops[cmv_ops.selftext != "[deleted]"]

rand_comments = pd.read_csv("../../data/work/samples/random_sample_comments_sample_1.tsv", sep="\t")
rand_comments = rand_comments[rand_comments.body != "[deleted]"]

rand_ops = pd.read_csv("../../data/work/samples/random_sample_submissions_sample_1.tsv", sep="\t")
rand_ops = rand_ops[rand_ops.selftext != "[deleted]"]

# split at whitespace, this works well enough
cmv_comments = cmv_comments['body'].map(lambda x: len(str(x).split())) 
rand_comments = rand_comments['body'].map(lambda x: len(str(x).split()))

cmv_op_lengths = cmv_ops['selftext'].map(lambda x: len(str(x).split()))
rand_op_lengths_all = rand_ops['selftext'].map(lambda x: len(str(x).split()))

# remove a couple of really long posts with more than 3000 words. these will be removed in the
# histogram, but included when calculating the mean and max values
# because the histogram would be very coarse-grained and hard to read imo
rand_op_lengths = rand_op_lengths_all[rand_op_lengths_all <= 3000]

print("cmv comments max and mean: ", cmv_comments.max(), cmv_comments.mean())
print("random comments max and mean: ", rand_comments.max(), rand_comments.mean())
print("cmv ops max and mean: ", cmv_op_lengths.max(), cmv_op_lengths.mean())
print("random ops max and mean: ", rand_op_lengths_all.max(), rand_op_lengths_all.mean())

fig, ax = plt.subplots(2,2)
fig.supxlabel('n of words')
fig.supylabel('frequency')
plt.title("Submission and comment length in CMV and random sample")

ax[0,0].hist(np.array(cmv_comments.array), bins=30)
ax[0,0].set_title('CMV comments')
ax[0,0].axvline(cmv_comments.array.mean(), color='k', linestyle='dashed', linewidth=1, label='mean')
ax[0,0].text(cmv_comments.array.mean()+3,3,round(cmv_comments.array.mean(),1))

ax[0,1].hist(np.array(rand_comments.array), bins=30)
ax[0,1].set_title('Random comments')
ax[0,1].axvline(rand_comments.array.mean(), color='k', linestyle='dashed', linewidth=1)
ax[0,1].text(rand_comments.array.mean()+3,3,round(rand_comments.array.mean(),1))


ax[1,0].hist(np.array(cmv_op_lengths.array), bins=30)
ax[1,0].set_title('CMV submissions')
ax[1,0].axvline(cmv_op_lengths.array.mean(), color='k', linestyle='dashed', linewidth=1)
ax[1,0].text(cmv_op_lengths.array.mean()+3,3,round(cmv_op_lengths.array.mean(),1))


ax[1,1].hist(np.array(rand_op_lengths.array), bins=30) # remove a couple posts that are 5000 words long
ax[1,1].set_title('Random submissions')
ax[1,1].axvline(rand_op_lengths_all.array.mean(), color='k', linestyle='dashed', linewidth=1)
ax[1,1].text(rand_op_lengths_all.array.mean()+3,3,round(rand_op_lengths_all.array.mean(),1))


plt.show()
