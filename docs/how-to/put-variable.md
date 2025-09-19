# How to Add Data to the Database

See below for guides on how to add variables to a database file. These guides assume you have `csdb` installed.

## From YAML

The expected format of the `yaml` file is a mapping of mappings (nested `dict`s in Python vocabulary). The keys of the outer mappings are ignored by `csdb`. The keys of the inner mapping should be:

- `name`: A human readable name for the variable
- `code_name`: The WRESL+ code name used in CalSim
- `kind`: The WRESL+ 'kind' used in CalSim
- `units`: The WRESL+ 'units' used in CalSim

These directly correspond to the arguments in the [`csbd.Client.put_variable`](../api/client.md#csdb.Client.put_variable) method. See below for an example `yaml` file.

```yaml
Shasta:
    name: Banks Exports
    code_name: C_CAA003
    kind: CHANNEL
    units: cfs
Oroville:
    name: Oroville Storage
    code_name: S_OROVL
    kind: STORAGE
    units: taf
```

The code below would add variables to a databse.

```python
import csdb
db = csdb.Client("foo.db")
db.put_variables_from_yaml("variables.yaml")
```

Additionally, you can use this `yaml` file as the initialization table when creating a new database file.

```python
import csdb
db = csdb.Client("foo.db", fill_vars_if_new="variables.yaml")
```

## From CSV

The expected format of the `csv` file is a table with one row per variable to be added. The following columns should be present.

- `name`: A human readable name for the variable
- `code_name`: The WRESL+ code name used in CalSim
- `kind`: The WRESL+ 'kind' used in CalSim
- `units`: The WRESL+ 'units' used in CalSim

These directly correspond to the arguments in the [`csbd.Client.put_variable`](../api/client.md#csdb.Client.put_variable) method. See below for an example `csv` file.

| name             | code_name | kind    | units |
| ----             | --------- | ----    | ----- |
| Banks Exports    | C_CAA003  | CHANNEL | cfs   |
| Oroville Storage | S_OROVL   | STORAGE | taf   |

The code below would add variables to a databse.

```python
import csdb
db = csdb.Client("foo.db")
db.put_variables_from_csv("variables.csv")
```

Additionally, you can use this `csv` file as the initialization table when creating a new database file.

```python
import csdb
db = csdb.Client("foo.db", fill_vars_if_new="variables.csv")
```