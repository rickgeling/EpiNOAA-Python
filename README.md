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

https://noaa-nclimgrid-daily-pds.s3.amazonaws.com/index.html#EpiNOAA/v1-0-0/csv/cen/

---

## How to anything related to noaa-nclimgrid-daily

This guide outlines the steps to set up the project environment and run the main processing script (e.g., `process_ag_weather_batched.py` or your `get_data_v3.py`) for the first time. This project uses Poetry for dependency and environment management.

### Phase 1: System-Level Prerequisites (Done Once)

Before you begin, ensure you have the following installed on your system:

1.  **Python:** A compatible version of Python (e.g., Python 3.9 or newer, as typically defined in the `pyproject.toml` file). You can download it from [python.org](https://www.python.org/downloads/).
2.  **Git:** Required to clone the repository. You can download it from [git-scm.com](https://git-scm.com/downloads).
3.  **Poetry:** The project's dependency manager. After installing Python and Pip, you can install Poetry by running:
    ```bash
    pip install poetry
    ```
    For more detailed instructions, visit [python-poetry.org](https://python-poetry.org/docs/#installation).
4.  **(For Windows Users, Potentially) Microsoft C++ Build Tools:** Some Python packages (especially scientific ones) may need to be compiled from C/C++ source during installation. If `poetry install` fails with errors related to a missing C++ compiler (like "Microsoft Visual C++ 14.0 or greater is required"), you'll need to install the [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/). When installing, select the "Desktop development with C++" workload.

### Phase 2: Project-Specific Setup (Done Once for the Project)

1.  **Clone Your Forked Repository:**
    Open your terminal or command prompt and navigate to where you want to store the project. Then, clone **your forked version** of the repository.
    ```bash
    git clone <YOUR_FORKED_REPOSITORY_URL>
    ```
    **Note:** Replace `<YOUR_FORKED_REPOSITORY_URL>` with the actual URL of your fork (e.g., `https://github.com/YourUsername/YourForkedRepoName.git`).

2.  **Navigate into the Project Directory:**
    Change your current directory to the cloned repository's folder. If your forked repository folder is named `EpiNOAA-Python` (or similar), you would type:
    ```bash
    cd YourForkedRepoName 
    ```
    (Replace `YourForkedRepoName` with the actual folder name).

3.  **Install Dependencies using Poetry:**
    This is the crucial step to create the project's isolated environment and install all necessary packages.
    ```bash
    poetry install
    ```
    * **What `poetry install` does:**
        * It reads the `pyproject.toml` file for dependency specifications.
        * If a `poetry.lock` file exists and is up-to-date, Poetry installs the exact package versions specified in it. This ensures a reproducible environment.
        * If `poetry.lock` doesn't exist or is outdated, Poetry first resolves all dependencies, finds a compatible set of specific versions, and writes these versions into the `poetry.lock` file (this is the "locking" step). It then installs these locked versions.
        * Poetry will also create an isolated virtual environment for this project if one doesn't already exist.

4.  **Ensure Necessary Data and Configuration Files are Present:**
    * **Yield Data CSV:** Make sure your input CSV file (e.g., `df_yield_weather.csv`) is placed in the correct location expected by the script (e.g., in the same directory as the main processing script, or as defined by `input_csv_path` within the script).
    * **Configuration File (`config.py`):** Ensure the `config.py` file (containing date windows, thresholds, etc.) is in the correct location so it can be imported by the main processing script (e.g., in the same directory, or in a subdirectory like `nclimgrid_importer` if imported as `from nclimgrid_importer import config`).

### Phase 3: Running the Script (Each Time a New Terminal is Opened)

Once the project is set up, you'll follow these steps each time you want to run your script in a new terminal session:

1.  **Navigate to the Project Directory:**
    Open your terminal and change to the project directory:
    ```bash
    cd path/to/YourForkedRepoName
    ```

2.  **Activate the Poetry Virtual Environment:**
    This ensures you are using the Python interpreter and packages specific to this project.
    * First, find the path to the virtual environment (you only need to do this once per machine if you forget the path):
        ```bash
        poetry env info --path
        ```
        This will output a path.
    * Then, activate the environment. The command depends on your operating system and shell:
        * **Windows (CMD/PowerShell):**
            ```bash
            <path_from_above>\Scripts\activate
            ```
            (e.g., `C:\Users\YourUser\AppData\Local\pypoetry\Cache\virtualenvs\project-name-abcdef-py3.x\Scripts\activate`)
        * **Linux/macOS (Bash/Zsh):**
            ```bash
            source <path_from_above>/bin/activate
            ```
    Your command prompt should change to indicate that the virtual environment is active (e.g., it might be prefixed with the environment name like `(YourForkedRepoName-py3.x)`).

3.  **Run the Python Script:**
    Now you can run your main processing script (e.g., `process_ag_weather_batched.py` or `get_data_v3.py`).
    ```bash
    python your_script_name.py
    ```
    If you want to save all the print statements and any errors to a log file, you can use terminal redirection:
    * **Windows:**
        ```bash
        python your_script_name.py > output_log.txt 2>&1
        ```
    * **Linux/macOS:**
        ```bash
        python your_script_name.py > output_log.txt 2>&1
        ```

These steps should allow anyone to set up the project from your fork and run the scripts.

