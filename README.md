# CalSim Database

> [!NOTE]  
> :construction: This library is still under development, and has no Anaconda or PyPI distribution yet.

A SQL tool for interacting with CalSim3 results

## Example Usage

```python
import csdb 

database_file = "database.db"
client = csdb.CalSimDatabaseClient(database_file)

run, variables, df = client.get_result_by_run(run_name="Baseline")
# Returns a DataFrame for a single run, but all variables in the database.

runs, variable, df = client.get_result_by_variable(run_name="S_OROVL")
# Returns a DataFrame for a single variable, but all runs in the database.
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
