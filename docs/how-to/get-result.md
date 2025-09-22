# How to Get Data from the Database

See below for guides on how to get run results from a database file. These guides assume you have `csdb` installed.

## All Variables for One Run

If you want to look at multiple outputs for a single run (kind of like pulling one whole DSS out of the database), you should use [`csdb.Client.get_result_by_run`](../api/client.md#csdb.Client.get_result_by_run).

```python
import csdb

client = csdb.Client("file.db")
run, variables, df = client.get_result_by_run(
    run_name="DCR 3000 - Baseline",
)
```

The data returned from this method is:

- A [`csdb.Run`](../api/schemas.md#csdb.Run) object that represents the metadata about the run that you queried.
- A `list` of [`csdb.Variable`](../api/schemas.md#csdb.Variable) objects that represent the metadata about the variables that the database had for this run.
- A `pandas.DataFrame` containing timeseries data for the run you queried.

The data might look like:

```cmd
>>> run
csdb.Run(name='DCR 3000 - Baseline', source='file.dss')

>>> variables
[csdb.Variable(name='Example Variable 1', code_name='VAR1'), csdb.Variable(...), ...]

>>> df
            VAR1    VAR2    VAR3
datetime
1921-10-31  12.3    1.23    0.123
1921-11-30  45.6    4.56    0.456
1921-12-31  78.9    7.89    0.789
...         ...     ...     ...
2021-09-30  78.9    7.89    0.789
```

## All Runs for One Variable

If you want to look at multiple runs for a single variable (kind of like reading multiple DSS files and concatenating the results), you should use [`csdb.Client.get_result_by_variable`](../api/client.md#csdb.Client.get_result_by_variable).


```python

import csdb

client = csdb.Client("file.db")
runs, variable, df = client.get_result_by_variable(
    code_name="S_OROVL",
)
```

The data returned from this method is:

- A `list` of [`csdb.Run`](../api/schemas.md#csdb.Run) objects that represent the metadata about the run that the database had for this variable.
- A [`csdb.Variable`](../api/schemas.md#csdb.Variable) object that represents the metadata about the variable that you queried.
- A `pandas.DataFrame` containing timeseries data for the variable you queried.

The data might look like:

```cmd
>>> run
[csdb.Run(name='RUN 1', source='file1.dss'), csdb.Run(...), ...]

>>> variables
csdb.Variable(name='Oroville Storage', code_name='S_OROVL')

>>> df
            RUN 1   RUN 2   RUN 2
datetime
1921-10-31  12.3    1.23    0.123
1921-11-30  45.6    4.56    0.456
1921-12-31  78.9    7.89    0.789
...         ...     ...     ...
2021-09-30  78.9    7.89    0.789
```
