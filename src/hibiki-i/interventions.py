# calculate inter-rater agreement between Samuli and Ylva

#%% #####################################################
from hereutil import here, add_to_sys_path
add_to_sys_path(here())
import pandas as pd
import numpy as np
from sklearn.metrics import confusion_matrix
import numpy as np
import matplotlib.pyplot as plt


# %% #####################################################
# Load the data
samuli3 = pd.read_csv(here('data/work/annotations/interventions-samuli-3.csv'))
ylva3 = pd.read_csv(here('data/work/annotations/interventions-ylva-3.csv'))

# Define the labels
map_labels = {'Yes': 1, 'No': 2, 'Ambiguous': 3, 'Disregard': 4}
samuli3['intervention'] = samuli3['intervention'].map(map_labels)
ylva3['intervention'] = ylva3['intervention'].map(map_labels)

# fill na
for row in samuli3.index:
    if np.isnan(samuli3['intervention'][row]):
        samuli3['intervention'][row] = ylva3['intervention'][row]
for row in ylva3.index:
    if np.isnan(ylva3['intervention'][row]):
        ylva3['intervention'][row] = samuli3['intervention'][row]

# one row was missing, annotated as 2
samuli3.fillna(2, inplace=True)
ylva3.fillna(2, inplace=True)

# Create the confusion matrix
matrix = confusion_matrix(samuli3['intervention'], ylva3['intervention'])

# Display the matrix
plt.imshow(matrix, cmap='viridis', interpolation='nearest')
for i in range(matrix.shape[0]):
    for j in range(matrix.shape[1]):
        plt.text(j, i, str(matrix[i, j]), ha='center', va='center', color='white')
plt.xticks(np.arange(matrix.shape[1]), ['Yes', 'No', 'Ambiguous', 'Disregard'])
plt.yticks(np.arange(matrix.shape[0]), ['Yes', 'No', 'Ambiguous', 'Disregard'])
plt.xlabel('Ylva')
plt.ylabel('Samuli')
plt.show()

# inter-rater agreement
from sklearn.metrics import cohen_kappa_score
kappa = cohen_kappa_score(samuli3['intervention'], ylva3['intervention'])
print(kappa)


# %% #####################################################
# use samuli-3 as the final result
samuli3['intervention'] = samuli3['intervention'].map({1: 'Yes', 2: 'No', 3: 'Ambiguous', 4: 'Disregard'})
samuli3.groupby('subreddit')['intervention'].value_counts()

# %%
