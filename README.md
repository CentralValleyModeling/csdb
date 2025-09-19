# CalSim Database

> [!NOTE]  
> :construction: This library is still under development, and has no Anaconda or PyPI distribution yet.

A SQL tool for interacting with CalSim3 results

## Example Usage

Reading data already in the Database.

```python
import csdb 

client = csdb.Client("database.db")

run, variables, df = client.get_result_by_run(run_name="Baseline")
# Returns a DataFrame for a single run, but all variables in the database.

runs, variable, df = client.get_result_by_variable(run_name="S_OROVL")
# Returns a DataFrame for a single variable, but all runs in the database.
```

Writing new data to the database from DSS files (DSS 6 and DSS 7)

```python
# This will read variables from the DSS file
# What variables? The database has a list of variables that it knows about, and
# it will use that to search the DSS file.
client.put_run_from_dss("New Run", "path/to/dss/file.dss")
```

Add variables to the codebase (do this before you add data from a DSS file)

```python
# CSV and YAML specifications are supported when creating a new database
# see src/csdb/default/variables.yaml for an example
client = csdb.Client(
    "new_database.db", 
    fill_vars_if_new="path/to/variables.csv",  
)

# or you can put them one by one
client.put_variable(
    name="Shasta Storage",
    code_name="S_SHSTA",
    kind="STORAGE",
    units="TAF",
)

# or you can add them in bulk to an existing database
client.put_variables_from_yaml("path/to/variables.yaml")
client.put_variables_from_csv("path/to/variables.csv")
client.put_variables_from_dataframe(df)
```

## Installation

> [!WARNING]  
> :construction: User-level installation is not completed yet.

## Developer Setup

Begin by cloning the repository:

```cmd
git clone https://github.com/CentralValleyModeling/csdb.git
```

Then, set up the conda environment with the developer dependencies.

```cmd
>>> conda env create -f environment.yaml -y
```

Then you're done!
