# DHH24 disc group project

## Introduction

See [BEST-PRACTICES.md](BEST-PRACTICES.md) for repository layout and coding best practices. Particularly, this repository has been setup to use [Poetry](https://python-poetry.org/) for Python dependency management and [renv](https://rstudio.github.io/renv/) for R dependency management. Additionally, a [Conda](https://conda.io/)-compatible tool (such as [micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html), [mamba](https://mamba.readthedocs.io/en/latest/installation.html) or [conda](https://docs.conda.io/projects/conda/en/stable/user-guide/install/download.html#anaconda-or-miniconda)) can be used to set up compatible versions of R, Python and Poetry if these are not already available on the system.

Thus, to get the project running, do the following:

1. Ensure you have compatible versions of R, Python and Poetry available on the system. If you have a conda-compatible tool, you can also use that to install these in an isolated environment, e.g. through `[micromamba/mamba/conda] create -f environment.yml -p ./.venv`.
2. Install R dependencies with `Rscript -e 'renv::restore()'`
3. Install Python dependencies with `poetry install`

## Data access

There are three basic ways to access the data:

1. Smaller random samples of each datasets are stored in the [`data/work/samples`](data/work/samples) folder in this repository as TSV files. These are the easiest way to use the data if a sample is sufficient for your analyses.
1. The master data is stored on a MariaDB database server. This can be very easily used from R through [dbplyr](https://dbplyr.tidyverse.org/), which in many scenarios allows you to use the data as if it were local through tidyverse verbs, transparently transforming them into SQL under the hood. For Python, Pandas can read from the database using SQL queries, but you need to write the SQL yourself.
1. Copies of the data are stored as parquet files in the Allas S3 service. In Python, Pandas can open these files through `read_parquet` in such a manner that only the parts of the data needed for a particular query are downloaded from S3, and additionally the whole dataset never needs to fit in memory at a time (You could do the same in R but there easier to use the database through dbplyr)

For either MariaDB or S3 access, you need a `secret.yaml` file which you can find in our shared google drive, and which you should put in the root directory of the repo.

For quick-start introductions on how to use the data in practice from code, look at [`src/example_user/example_analysis.Rmd`](src/example_user/example_analysis.md) and[`src/example_user/example_analysis.ipynb`](src/example_user/example_analysis.ipynb), as well as the various things under [`src/jiemakel/`](src/jiemakel/).

## Using this repo inside the CSC environment

There are additional scripts and knacks for using this repository in the various CSC environments. If you need to do this, ask for help from Eetu.
