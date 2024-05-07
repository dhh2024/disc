# Introduction

See [BEST-PRACTICES.md](BEST-PRACTICES.md) for repository layout and coding best practices. Particularly, this repository has been setup to use [Poetry](https://python-poetry.org/) for Python dependency management and [renv](https://rstudio.github.io/renv/) for R dependency management. Additionally, a [Conda](https://conda.io/)-compatible tool (such as [micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html), [mamba](https://mamba.readthedocs.io/en/latest/installation.html) or [conda](https://docs.conda.io/projects/conda/en/stable/user-guide/install/download.html#anaconda-or-miniconda)) can be used to set up compatible versions of R, Python and Poetry if these are not already available on the system.

Thus, to get the project running, do the following:

1. Ensure you have compatible versions of R, Python and Poetry available on the system. If you have a conda-compatible tool, you can also use that to install these in an isolated environment, e.g. through `[micromamba/mamba/conda] create -f environment.yml -p ./.venv`.
2. Install R dependencies with `Rscript -e 'renv::restore()'`
3. Install Python dependencies with `poetry install`

For quick-start introductions on how to use the data in practice from code, look at [`src/example_user/example_analysis.Rmd`](src/example_user/example_analysis.md) and[`src/example_user/example_analysis.ipynb`](src/example_user/example_analysis.ipynb). 

