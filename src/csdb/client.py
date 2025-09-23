import logging
import tempfile
from csv import QUOTE_NONNUMERIC
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd

from . import io, schemas

logger = logging.getLogger(__name__)
EXPECTED_RESULT_DF_COLUMNS = [
    "run",
    "datetime",
    "variable",
    "value",
]

SCHEMA_DIR = Path(__file__).parent / "default" / "sql" / "schema"
DEFAULT_VARIABLES_YAML = Path(__file__).parent / "default" / "variables.yaml"


class Client:
    """`csdb.Client` is database client for CalSim modeling results that uses `duckdb`

    `Client` provides multiple convienient methods that themselves execute SQL queries
    to read, add, and remove data from the duckdb database on disk. The methods are
    simply convieniences in interacting with this database.

    Parameters
    ----------
    src : Path | str
        The path to the database file.
    fill_vars_if_new : bool | Path | None, optional
        If Truthy, the client will fill the variable table when initializing the
        database. If True, the client uses the default variable list for CalSim
        runs. If a Path is given, that path is interpreted as the yaml file to use
        as a source of the variables, by default True
    schema_directory : Path | None, optional
        The path to the directory that contains the SQL schema definition files if
        you want to extend the default schema, by default None

    Example
    -------
    Connect to an existing, or create a new database. For new dataabses, this
    will initialize the variable table with a standard set of CalSim3 Variables.

        >>> import csdb
        >>> client = csdb.Client("file.db")

    Create a a new database without initializing the variable table.

        >>> import csdb
        >>> client = csdb.Client("file.db", fill_vars_if_new=False)

    Specify the yaml or csv source file to use when initalizing the variables.

        >>> import csdb
        >>> client = csdb.Client(src="file.db", fill_vars_if_new="variables.yaml")

    Raises
    ------
    IOError
        Raised if `fill_vars_if_new` cannot be interpreted for the variable table
    """

    def __init__(
        self,
        src: Path | str,
        fill_vars_if_new: bool | Path = True,
        schema_directory: Path | None = None,
    ):
        src = Path(src)
        # resolve default mutable arguments
        if not isinstance(fill_vars_if_new, bool):
            variables_src_file = Path(fill_vars_if_new)
            fill_vars_if_new = True
        else:
            variables_src_file = DEFAULT_VARIABLES_YAML
        if schema_directory is None:
            schema_directory = SCHEMA_DIR
        # Set attributes on the client
        if not src.is_absolute():
            src = Path(".").resolve() / src
        self.__src: Path = src
        # Create, initialize, and possibly fill the database if it doesn't exist.
        if not self.__src.exists():
            logger.debug(f"creating new database file: {self.__src}")
            with duckdb.connect(self.__src, read_only=False) as conn:
                for f in schema_directory.iterdir():
                    if f.suffix != ".sql":
                        continue
                    conn.sql(f.read_text())
            if fill_vars_if_new:
                var_parse_methods = {
                    ".csv": self.put_variables_from_csv,
                    ".yaml": self.put_variables_from_yaml,
                    ".yml": self.put_variables_from_yaml,
                }
                if variables_src_file.suffix not in var_parse_methods:
                    raise IOError(
                        f"Cannot interpret {variables_src_file.suffix} when "
                        + "initializing database with variables, only the following "
                        + f"file types are supported: {list(var_parse_methods.keys())}"
                    )
                method = var_parse_methods[variables_src_file.suffix]
                method(variables_src_file)

    def put_variables_from_yaml(self, src: Path):
        """Put variables into the databse that are descibed in a yaml file.

        The yaml file be a yaml dictionary of dictionaries, the top level keys are not
        used in the database interaction.

        The nested dictionaries should have the following keys:
        - `name`: A human readable name for the variable
        - `code_name`: The WRESL+ code name used in CalSim
        - `kind`: The WRESL+ 'kind' used in CalSim
        - `units`: The WRESL+ 'units' used in CalSim

        Parameters
        ----------
        src : Path
            The path to the yaml source file

        Example
        -------
        Given a yaml file like this:

        ```yaml title="file.yaml"
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

        Add that data to the database like this:

            >>> import csdb
            >>> client = csdb.Client("foo.db")
            >>> client.put_variables_from_yaml("file.yaml")
        """
        logger.info(f"adding variables from yaml={src}")
        obj: dict = io.load_yaml(src)
        str_dss_path = list()
        s = "(nextval('seq_variable'), '{name}', '{code_name}', '{kind}', '{units}')"
        added = list()
        for k, v in obj.items():
            str_dss_path.append(s.format(**v))
            added.append(k)
        with duckdb.connect(self.__src, read_only=False) as conn:
            q = f"""INSERT INTO variable
                    VALUES {', '.join(str_dss_path)}
                    ON CONFLICT DO NOTHING;"""
            conn.sql(q)
        logger.debug(f"added the following entries: {', '.join(added)}")

    def put_variables_from_dataframe(self, df: pd.DataFrame):
        """Put variables into the databse that are descibed in a DataFrame.

        The table should have the following columns:
        - `name`: A human readable name for the variable
        - `code_name`: The WRESL+ code name used in CalSim
        - `kind`: The WRESL+ 'kind' used in CalSim
        - `units`: The WRESL+ 'units' used in CalSim

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe of variables to be added.

        Example
        -------
        Given a dataframe like this:

        | name             | code_name | kind    | units |
        | ----             | --------- | ----    | ----- |
        | Banks Exports    | C_CAA003  | CHANNEL | cfs   |
        | Oroville Storage | S_OROVL   | STORAGE | taf   |

        Add that data to the database like this:

            >>> import csdb
            >>> client = csdb.Client("foo.db")
            >>> df = pd.DataFrame(...)
            >>> client.put_variables_from_dataframe(df)
        """
        df.columns = [c.lower() for c in df.columns]
        logger.info(f"adding {len(df)} variables from dataframe")
        str_dss_path = list()
        s = "(nextval('seq_variable'), '{name}', '{code_name}', '{kind}', '{units}')"
        added = list()
        for _, row in df.iterrows():
            str_dss_path.append(s.format(**row.to_dict()))
            added.append(row["name"])
        with duckdb.connect(self.__src, read_only=False) as conn:
            q = f"""INSERT INTO variable
                    VALUES {', '.join(str_dss_path)}
                    ON CONFLICT DO NOTHING;"""
            conn.sql(q)
        logger.debug(f"added the following entries: {', '.join(added)}")

    def put_variables_from_csv(self, src: Path):
        """Put variables into the databse that are descibed in a csv.

        The csv should have the following columns:
        - `name`: A human readable name for the variable
        - `code_name`: The WRESL+ code name used in CalSim
        - `kind`: The WRESL+ 'kind' used in CalSim
        - `units`: The WRESL+ 'units' used in CalSim

        Parameters
        ----------
        src : Path
            The csv of variables to be added.

        Example
        -------
        Given a csv like this:

        ```csv title="file.csv"
        name,code_name,kind,units
        Banks Exports,C_CAA003,CHANNEL,cfs
        Oroville Storage,S_OROVL,STORAGE,taf
        ```

        Add that data to the database like this:

            >>> import csdb
            >>> client = csdb.Client("foo.db")
            >>> client.put_variables_from_csv("file.csv")
        """
        logger.info(f"adding variables from csv={src}")
        df = pd.read_csv(src)
        self.put_variables_from_dataframe(df)

    def put_variable(self, name: str, code_name: str, kind: str, units: str):
        """Put a single variable into the database

        Parameters
        ----------
        name: str
            A human readable name for the variable
        code_name: str
            The WRESL+ code name used in CalSim
        kind: str
            The WRESL+ 'kind' used in CalSim
        units: str
            The WRESL+ 'units' used in CalSim

        Example
        -------
            >>> import csdb
            >>> client = csdb.Client("foo.db")
            >>> client.put_variable(
                name="New Variable",
                code_name="NEWVAR",
                kind="EXAMPLE",
                units="OLYMPICSWIMMINGPOOLS"
            )
        """
        with duckdb.connect(self.__src, read_only=False) as conn:
            data = f"'{name}', '{code_name}', '{kind}', '{units}'"
            q = f"""INSERT INTO variable
                    VALUES (nextval('seq_variable'), {data})
                    ON CONFLICT DO NOTHING;"""
            conn.sql(q)

    def get_variable(self, code_name: str) -> schemas.Variable:
        """Get a variable from the database.

        Parameters
        ----------
        code_name : str
            The WRESL+ name used for the variable

        Returns
        -------
        schemas.Variable
            The Variable object that contains the metadata for that CalSim variable

        Example
        -------
            >>> import csdb
            >>> client = csdb.Client("foo.db")
            >>> variable = client.get_variable(code_name="S_OROVL")
            csdb.Variable(name="Oroville Storage", code_name="S_OROVL", ...)

        Raises
        ------
        ValueError
            Raised when the variable cannot be found,
        """
        query = (
            "SELECT name, code_name, kind, units FROM variable"
            + f" WHERE code_name = '{code_name}'"
        )
        with duckdb.connect(self.__src, read_only=True) as conn:
            tbl = conn.sql(query)
            df = tbl.to_df()
            if len(df) != 1:
                logger.error(tbl)
                raise ValueError(
                    f"Query didn't return len(df) == 1 for {code_name=}\n{df}"
                )
        return schemas.Variable.model_validate(df.loc[0, :].to_dict())

    def get_table_as_dataframe(
        self,
        table_name: Literal["run", "variable", "result"],
    ) -> pd.DataFrame:
        """Return a DataFrame copy of a database table.

        Parameters
        ----------
        table_name : Literal[&quot;run&quot;, &quot;variable&quot;, &quot;result&quot;]
            The name of the table within the database

        Returns
        -------
        pd.DataFrame
            A copy of the table as a DataFrame. Modifications to this table will not
            impact the database

        Example
        -------
            >>> import csdb
            >>> client = csdb.Client("foo.db")
            >>> variable = client.get_table_as_dataframe(table_name="run")
                name    source
            0   RUN1    source/file/for/the/run1.dss
            1   RUN2    source/file/for/the/run2.dss

        Raises
        ------
        ValueError
            Raised if the table name provided isn't allowed.
        """
        logger.debug(f"getting table {table_name}")
        viewable = ("run", "variable", "result")
        if table_name not in viewable:
            raise ValueError(f"expected one of {viewable}, got {table_name=}")
        with duckdb.connect(self.__src, read_only=True) as conn:
            df = conn.sql(f"SELECT * FROM {table_name};").to_df()
        return df

    def get_variable_counts(self) -> pd.Series:
        """Get the number of variables present in each run in the database.

        Returns
        -------
        pd.DataFrame
            The table relating run.name and the count of unique variable.code_name.

        Example
        -------
            >>> import csdb
            >>> client = csdb.Client("foo.db")
            >>> client.get_variable_counts
                        variable_counts
            run_name
            RUN1        42
            RUN2        256
        """
        with duckdb.connect(self.__src, read_only=True) as conn:
            s = (
                conn.execute(
                    """SELECT run.name AS run_name,
                    COUNT(DISTINCT variable.code_name) AS code_name_count
                FROM result
                JOIN run ON result.run_id = run.id
                JOIN variable ON result.variable_id = variable.id
                GROUP BY run.name
                ORDER BY run.name;"""
                )
                .df()
                .set_index("run_name")
                .loc[:, "code_name_count"]
            )
        s.name = "variable_counts"
        return s

    def _put_run(self, name: str, source: str | Path, ignore_conflict: bool = True):
        source = str(source)
        with duckdb.connect(self.__src, read_only=False) as conn:
            if ignore_conflict:
                q = f"""INSERT INTO run
                        VALUES (nextval('seq_run'), '{name}', '{source}')
                        ON CONFLICT DO NOTHING;"""
            else:
                q = f"""INSERT INTO run
                        VALUES (nextval('seq_run'), '{name}', '{source}');"""
            conn.sql(q)

    def _get_run(self, run_name: str) -> schemas.Run:
        with duckdb.connect(self.__src, read_only=True) as conn:
            tbl = conn.sql(
                f"SELECT name, source FROM run WHERE run.name = '{run_name}';"
            )
            df = tbl.to_df()
        if len(df) > 1:
            logger.error(tbl)
            raise ValueError(f"More than one object returned for {run_name=}\n{df}")
        return schemas.Run.model_validate(df.loc[0, :].to_dict())

    def put_run_from_dataframe(
        self,
        run_name: str,
        df: pd.DataFrame,
        src: str | Path | None = None,
    ) -> tuple[schemas.Run, list[schemas.Variable], pd.DataFrame]:
        """Add a run and its results to the database.

        If the run already exists, the results will be appended to the database. No
        overlaps are allowed and will throw an error.

        Parameters
        ----------
        run_name : str
            The name to use for the run in the database
        df : pd.DataFrame
            The DataFrame containing the data for the run, should have columns
            'datetime', 'variable', and 'value'
        src : str | Path, optional
            The source to record for the run, if it isn't given it's recorded as the
            DataFrames memory location, by default None

        Returns
        -------
        tuple[schemas.Run, list[schemas.Variable], pd.DataFrame]
            The Run obejct, Variable objects, and pivoted DataFrame for the Run affected

        Example
        -------
            >>> import csdb
            >>> import pandas as pd
            >>> client = csdb.Client("foo.db")
            >>> df = pd.DataFrame(...)
            >>> client.put_run_from_dataframe("Example Run", df, src="Custom calcs")

        Raises
        ------
        ValueError
            Raised if there are variables in the DataFrame that aren't already known in
            the database. Try adding them using `client.put_variable(...)` first.
        """
        if src is None:
            src = str(f"{df.__class__.__name__}@{id(df)}")
        else:
            src = str(src)
        df.columns = [c.lower() for c in df.columns]  # clean up capitalization
        df = df.loc[:, EXPECTED_RESULT_DF_COLUMNS]  # clean up the order
        with duckdb.connect(self.__src, read_only=False) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO run (id, name, source) "
                + "VALUES (nextval('seq_run'), ?, ?) "
                + "RETURNING id",
                [run_name, src],
            )
            added = conn.execute(
                f"SELECT id FROM run WHERE run.name = '{run_name}'"
            ).fetchone()
            if added is None:
                raise duckdb.DataError(f"Couldn't find {run_name=}")
            run_id = added[0]
            # Resolve all variable -> id in one query
            variable_names = df["variable"].unique().tolist()
            var_map = {
                row[0]: row[1]
                for row in conn.execute(
                    "SELECT code_name, id FROM variable WHERE code_name IN ({})".format(
                        ",".join("?" * len(variable_names))
                    ),
                    variable_names,
                ).fetchall()
            }

            # Check if any variables are missing
            missing = set(variable_names) - set(var_map.keys())
            if missing:
                raise ValueError(f"Variables not found in DB: {missing}")

            # Prepare dataframe for batch insert
            df["variable_id"] = df["variable"].map(var_map)
            df["run_id"] = run_id
            df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d")
            df = df.loc[:, ["datetime", "value", "run_id", "variable_id"]]
            df = df.sort_values(by=["variable_id", "run_id", "datetime"])
            with tempfile.TemporaryDirectory(prefix="csdb") as TEMP:
                csv_path = Path(TEMP) / f"{id(run_id)}.csv"
                logger.debug(f"copying data to database, using temp: {csv_path}")
                df.to_csv(csv_path, index=False, quoting=QUOTE_NONNUMERIC, header=False)
                conn.execute(f"COPY result from '{csv_path}' (DATEFORMAT '%Y-%m-%d');")
        return self.get_result_by_run(run_name=run_name)

    def put_run_from_dss(
        self,
        run_name: str,
        src: str | Path,
    ) -> tuple[schemas.Run, list[schemas.Variable], pd.DataFrame]:
        """Add a run and its results to the database.

        Each variable in the database is looked for in the DSS, overlapping variables
        are read and saved.

        Parameters
        ----------
        run_name : str
            The name to use for the run in the database
        src : str | Path
            The path to the DSS file to read

        Returns
        -------
        tuple[schemas.Run, list[schemas.Variable], pd.DataFrame]
            The Run obejct, Variable objects, and pivoted DataFrame for the Run affected

        Example
        -------
            >>> import csdb
            >>> client = csdb.Client("foo.db")
            >>> client.put_run_from_dss("Example Run", src="foo.dss")
        """
        # Get the current set of variables to know what datasets to read from the DSS
        df_variables = (
            self.get_table_as_dataframe("variable")
            .set_index("code_name", drop=False)
            .rename(
                columns={"id": "variable_id", "code_name": "code_name"},
            )
        )
        paths = io.get_intersecting_catalog(src, df_variables.index)
        df = io.load_dss(src, paths)
        df["run"] = run_name

        return self.put_run_from_dataframe(run_name, df, src=src)

    def delete_run(self, run_name: str) -> None:
        """Remove a run and it's result data from the database.

        Parameters
        ----------
        run_name : str
            The name of the run to remove

        Example
        -------
            >>> import csdb
            >>> client = csdb.Client("foo.db")
            >>> client.delete_run("BAD_RUN_OOPS")
        """
        with duckdb.connect(self.__src, read_only=False) as conn:
            # First delete results
            conn.execute(
                """
                DELETE FROM result
                WHERE run_id = (SELECT id FROM run WHERE name = ?)
                """,
                [run_name],
            )
            # Then delete the run itself
            conn.execute(
                "DELETE FROM run WHERE name = ?",
                [run_name],
            )

    def get_result_by_run(
        self,
        run_name: str,
    ) -> tuple[schemas.Run, list[schemas.Variable], pd.DataFrame]:
        """Get all the data out of the database for a single run.

        Parameters
        ----------
        run_name : str
            The name of the run to get data for

        Returns
        -------
        tuple[schemas.Run, list[schemas.Variable], pd.DataFrame]
            The Run obejct, Variable objects, and pivoted DataFrame of the data added

        Example
        -------
            >>> import csdb
            >>> client = csdb.Client("file.db")
            >>> run, variables, df = client.get_result_by_run(
                run_name="DCR 3000 - Baseline",
            )
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
        """
        run_obj = self._get_run(run_name)
        with duckdb.connect(self.__src, read_only=True) as conn:
            q = f"""SELECT
                    result.datetime AS datetime,
                    variable.code_name AS variable,
                    result.value AS value,
                FROM result
                JOIN run ON result.run_id = run.id
                JOIN variable ON result.variable_id = variable.id
                WHERE run.name = '{run_name}';"""
            df = conn.sql(q).to_df()
        df = df.pivot(index="datetime", columns="variable", values="value")
        variables = [self.get_variable(code_name=v) for v in df.columns.unique()]
        return run_obj, variables, df

    def get_result_by_variable(
        self,
        code_name: str,
    ) -> tuple[list[schemas.Run], schemas.Variable, pd.DataFrame]:
        """Get all the data out of the database for a single variable.

        Parameters
        ----------
        code_name : str
            The name of the variable to get data for, using the WRESL+ name

        Returns
        -------
        tuple[list[schemas.Run], schemas.Variable, pd.DataFrame]
            The Run obejcts, Variable object, and pivoted DataFrame of the data added

        Example
        -------
            >>> import csdb
            >>> client = csdb.Client("file.db")
            >>> run, variables, df = client.get_result_by_variable(
                code_name="S_OROVL",
            )
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
        """
        variable_object = self.get_variable(code_name=code_name)
        with duckdb.connect(self.__src, read_only=True) as conn:
            q = f"""SELECT
                    run.name AS run,
                    result.datetime AS datetime,
                    result.value AS value,
                FROM result
                JOIN run ON result.run_id = run.id
                JOIN variable ON result.variable_id = variable.id
                WHERE variable.code_name = '{variable_object.code_name}';"""

            df = conn.sql(q).to_df()
        df = df.pivot(index="datetime", columns="run", values="value")
        runs = [self._get_run(run_name=v) for v in df.columns.unique()]
        return runs, variable_object, df
