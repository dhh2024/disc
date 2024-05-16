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

## More on the database

Each table in the MariaDB database has a suffix, either `_a` or `_c`, which indicates the storage engine ([Aria](https://mariadb.com/kb/en/aria-storage-engine/) or [ColumnStore](https://mariadb.com/docs/columnstore/)) backing that table. The main relevance of the storage engine is that:

1. ColumnStore tables in general perform better when you need to count the number of entries in large while Aria tables perform better when you need to extract a small subset (but sometimes not, so in the end, if something is slow, try the other engine!)
1. Only Aria tables have [full-text indices](https://mariadb.com/kb/en/full-text-index-overview/) for efficient text search (this has its own syntax, check the docs).
1. Cross-engine joins are terrible, so avoid them if you can.

## Data model

Main data is in `[prefix]_submissions_[a|c]` and `[prefix]_comments_[a|c]`, with prefix being (at the time of writing) one of `[cmw|eli5|aita|stop_arguing|best_of_reddit|random_sample]`.

The schema of the submissions table is:

- subreddit_id BIGINT UNSIGNED NOT NULL, -- the numeric id of the subreddit
- subreddit VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL, -- the name of the subreddit
- id BIGINT UNSIGNED NOT NULL PRIMARY KEY, -- the unique numeric id of the submission
- permalink VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL, -- an URL to the submission
- created_utc TIMESTAMP NOT NULL, -- the time the submission was created (in the UTC timezone)
- author_id BIGINT UNSIGNED, -- the numeric id of the author, if available
- author VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL, -- the username of the author
- title VARCHAR(510) CHARACTER SET utf8mb4 NOT NULL, -- the title of the submission
- url VARCHAR(510) CHARACTER SET utf8mb4, -- an URL if the submission was a link submission
- selftext TEXT CHARACTER SET utf8mb4, -- the text of the submission if it wasn't a link-only submission
- score INTEGER NOT NULL, -- the score of the submission as calculated by subtracting the number of downvotes from the number of upvotes
- num_comments INTEGER UNSIGNED NOT NULL, -- the number of comments on the submission as reported by Reddit
- upvote_ratio FLOAT -- the ratio of the number of upvotes to the number of downvotes. Not available for old data.

The schema of the comments table is:

- subreddit_id BIGINT UNSIGNED NOT NULL, -- the numeric id of the subreddit
- subreddit VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL, -- the name of the subreddit
- id BIGINT UNSIGNED NOT NULL PRIMARY KEY, -- the unique numeric id of the comment
- permalink VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL, -- an URL to the comment
- link_id BIGINT UNSIGNED NOT NULL, -- the id of the submission this comment belongs to
- parent_comment_id BIGINT UNSIGNED, -- the id of the parent comment. If not given, the parent is the submission (link_id)
- created_utc TIMESTAMP NOT NULL, -- the time the comment was created (in the UTC timezone)
- author_id BIGINT UNSIGNED, -- the numeric id of the author, if available
- author VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL, -- the username of the author
- body TEXT CHARACTER SET utf8mb4, -- the text of the comment (quoted sections begin with &gt;)
- score INTEGER NOT NULL, -- the score of the comment as calculated by subtracting the number of downvotes from the number of upvotes
- controversiality BOOLEAN -- comments with lots of up and downvotes are considered controversial by Reddit

Additionally, there are some downstream tables for various purposes. These are:

- `cmw_delta_comments_a` contains all comments in ChangeMyView where the OP gives a delta. This can be used to find the comment receiving the delta through `parent_comment_id`, and the whole discussion using `link_id`.
- Base data samples also have a copy in the database, with the suffix `_sample_[number]`.
- The samples have also been automatically linguistically parsed with [Stanza](https://stanfordnlp.github.io/stanza/). The parsed data is in tables with the suffix `_parse`, detailed further below.

## Data model for linguistically parsed data

The parsed data is in tables with the suffix `_parse`. The schema of the main parse tables is:

## Using this repo inside the CSC environment

There are additional scripts and knacks for using this repository in the various CSC environments. If you need to do this, ask for help from Eetu.
