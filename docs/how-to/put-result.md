# How to Add Data to the Database

See below for guides on how to add run results to a database file. These guides assume you have `csdb` installed.

=== "From DSS"

    Generally, adding data from DSS is pretty straight-forward. At a high level, the following steps should be all you need to do.

    1. Specify the variables you want to extract from the DSS file.
    2. Copy data from the DSS file using [`csdb.Client.put_run_from_dss`](../api/client.md#csdb.Client.put_run_from_dss).

    Step 1 can be skipped if you want to use the [default `variable`](../api/default-variables.md) table. If you want to use a custom variable table, you have two options.

    - Initialize and empty database, and then add the variables using the [`csdb.Client.put_variable`](../api/client.md#csdb.Client.put_variable) method.
    - Create your own specification file in either the CSV or YAML format. If you choose this option, see the [how to create your own variable table](put-variable.md) page.

    The rest of this how-to guide assumes you want to use the default list of variables (we skip to step 2 listed above).

    ```python
    import csdb

    client = csdb.Client("file.db")
    run, variables, df = client.put_run_from_dss(
        run_name="DCR 3000 - Baseline",
        src="path/to/file.dss"
    )
    ```

    This will look at the DSS file (version 6 and 7 supported) for each of the  `code_name` values from the [`variable`](../api/sql.md#variable) table, checking the DSS catalog B parts for matches. For found matches, the data will be copied into the [`result`](../api/sql.md#result) table.

=== "From a `DataFrame`"

    Adding data from a `DataFrame` is a little more complex than adding it from DSS, in that you need to make sure the `DataFrame` is formatted correctly. The format expected is:

    | datetime                   | value           | variable       |
    | --------                   | -----           | --------       |
    | 1921-10-30 (`pd.Datetime`) | 1.23 (`float`)  | V_VAR (`str`)  |

    Additionally, the following requirements must also be met:

    1. Combinations of `datetime` and `variable` cannot be duplicated.
    2. The `variable` must be a known `code_name` in the [`variable`](../api/sql.md#variable) table.
    3. If the `run_name` specified is already in the database, the data in the database will be overwritten.
    4. If you don't provide a `src` argument, the client will just record the memory id for the dataframe that you provide. This won't be very helpful if you want to figure out where the data came from later.

    ```python
    import csdb
    import pandas as pd

    df = pd.DataFrame(...)
    client = csdb.Client("file.db")
    run, variables, df = client.put_run_from_dataframe(
        run_name="DCR 3000 - Baseline",
        df=df,
        src="Upload from DataFrame by Sir Robin"
    )
    ```
