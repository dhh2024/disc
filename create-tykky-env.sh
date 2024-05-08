#!/bin/sh
# create a tykky container venv in CSC Puhti/Mahti
module purge
module load git-lfs
module load tykky
mkdir venv
conda-containerize new --prefix venv cuda-env.yml

