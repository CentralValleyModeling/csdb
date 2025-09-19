
# Database Schema

## Table Definitions

### `run`

```sql
CREATE TABLE IF NOT EXISTS run (
        id INTEGER,
        name VARCHAR NOT NULL, 
        source VARCHAR NOT NULL,
        PRIMARY KEY (id), 
        UNIQUE (name)
);
```

#### Example `run` table

| id | name                          | source                           |
| -- | ----                          | ------                           |
| 0  | DCR 2023 Baseline             | Path/to/source/file/baseline.dss |
| 1  | DCR 2023 95% Level of Concern | Path/to/source/file/95LOC.dss    |

### `variable`

```sql
CREATE TABLE IF NOT EXISTS variable (
        id INTEGER,
        name VARCHAR NOT NULL, 
        code_name VARCHAR NOT NULL, 
        kind VARCHAR NOT NULL, 
        units VARCHAR NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT unique_variable UNIQUE (code_name)
);
```

#### Example `variable` table

| id | name             | code_name | kind    | units |
| -- | ----             | --------- | ----    | ----- |
| 0  | Oroville Storage | S_OROVL   | STORAGE | TAF   |
| 1  | Shasta Storage   | S_SHSTA   | STORAGE | TAF   |
| 2  | Banks Exports    | C_CAA003  | CHANNEL | CFS   |

### `result`

```sql
CREATE TABLE IF NOT EXISTS result (
        datetime DATE NOT NULL, 
        value FLOAT NOT NULL, 
        run_id INTEGER NOT NULL, 
        variable_id INTEGER NOT NULL, 
        CONSTRAINT unique_datapoint UNIQUE (run_id, variable_id, datetime), 
        FOREIGN KEY(run_id) REFERENCES run (id), 
        FOREIGN KEY(variable_id) REFERENCES variable (id)
);
```

#### Example `result` table

| datetime     | value   | run_id | variable_id |
| --------     | ----    | -----  | ----------- |
| `1921-10-31` | 2,268.5 | 0      | 0           |
| `1921-11-30` | 2,206.2 | 0      | 0           |
| `1921-12-31` | 2,302.2 | 0      | 0           |
| `1922-01-31` | 2,323.5 | 0      | 0           |
| `1922-02-28` | 2,648.1 | 0      | 0           |
| `1922-03-31` | 2,819.9 | 0      | 0           |
| `1922-04-30` | 3,336.3 | 0      | 0           |
| `1922-05-31` | 3,425.2 | 0      | 0           |
| `1922-06-30` | 3,425.2 | 0      | 0           |
| `1922-07-31` | 2,949.3 | 0      | 0           |
| `1922-08-31` | 2,478.2 | 0      | 0           |
| `1922-09-30` | 2,330.5 | 0      | 0           |
| ...          | ...     | ...    | ...         |
| `1921-10-31` | 2,372.8 | 1      | 2           |
| `1921-11-30` | 5,206.2 | 1      | 2           |
| `1921-12-31` | 3,612.0 | 1      | 2           |
| `1922-01-31` | 2,929.0 | 1      | 2           |
| `1922-02-28` | 4,743.3 | 1      | 2           |
| `1922-03-31` | 5,638.5 | 1      | 2           |
| `1922-04-30` | 614.0   | 1      | 2           |
| `1922-05-31` | 787.0   | 1      | 2           |
| `1922-06-30` | 2,543.6 | 1      | 2           |
| `1922-07-31` | 7,180.0 | 1      | 2           |
| `1922-08-31` | 4,417.5 | 1      | 2           |
| `1922-09-30` | 2,580.2 | 1      | 2           |
