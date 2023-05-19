# NClimGrid Importer

## Executive Summary

 The analysis ready NClimGrid data made available through the CISESS ARC project and the NOAA Open Data Dissemination program allow fo  r parallel performant access to NClimGrid data. Importing this data leveraging the features of the underlying storage format means th  at only the data requested is loaded into memory for analysis.

## Getting Started

 To get started, first check out the code available [here](https://gitlab.cicsnc.org/arc-project/nclimgrid-importer).

 This repository contains a to-be-published python package with hooks to the NODD AWS NClimGrid repository that facilates fast access   to the data.

 We will primarily be using the `load_nclimgrid_data` function located under the `nclimgrid_importer` directory in the repository.

 This function will allow us to specify the time periods we want to examine, the spatial resolution we want our data, and the specific   spatial areas that we want to analyze.                                                                                              
 First, set up your environment.  If you are working in this cloned repo, consult the Makefile for easy setup:

 ```
 make install
 ```

 If not, it is recommended to set up an isolated python environment with the dependencies listed in the `pyproject.toml` file.

 > __Dependencies__ This project uses [Poetry](https://python-poetry.org/) as its environment and package manager. Please install it g  lobally if you have not done so already.  The NClimGrid data are available most rapidly in parquet format.  To interface with this fo  rmat, you may need to install some system libraries to access these data.  On most systems, running `poetry install` should take care   of it.
