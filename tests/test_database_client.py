from pathlib import Path

import pandas as pd
import pytest

import csdb


def test_default_variables(
    temp_database_path: Path,
):
    client = csdb.CalSimDatabaseClient(temp_database_path)
    df = client.get_table_as_dataframe("variable")
    assert len(df) == 449


def test_init_variables_from_yaml(
    temp_database_path: Path,
    assets_dir: Path,
):
    var_file = assets_dir / "existing/variables_new.yaml"
    client = csdb.CalSimDatabaseClient(temp_database_path, fill_vars_if_new=var_file)
    df = client.get_table_as_dataframe("variable")
    assert len(df) == 2


def test_init_variables_from_csv(
    temp_database_path: Path,
    assets_dir: Path,
):
    var_file = assets_dir / "existing/variables_new.csv"
    client = csdb.CalSimDatabaseClient(temp_database_path, fill_vars_if_new=var_file)
    df = client.get_table_as_dataframe("variable")
    assert len(df) == 2


def test_init_variables_empty(
    temp_database_path: Path,
):
    client = csdb.CalSimDatabaseClient(temp_database_path, fill_vars_if_new=False)
    df = client.get_table_as_dataframe("variable")
    assert len(df) == 0


def test_put_variables_from_yaml(
    temp_database_path: Path,
    assets_dir: Path,
):
    client = csdb.CalSimDatabaseClient(temp_database_path, fill_vars_if_new=False)
    original_len = len(client.get_table_as_dataframe("variable"))
    client.put_variables_from_yaml(assets_dir / "existing/variables_new.yaml")
    variable = client.get_variable(code_name="TEST1")
    assert variable.name == "Test Variable 1"
    df = client.get_table_as_dataframe("variable")
    assert len(df) == original_len + 2


def test_put_variables_from_csv(
    temp_database_path: Path,
    assets_dir: Path,
):
    client = csdb.CalSimDatabaseClient(temp_database_path, fill_vars_if_new=False)
    original_len = len(client.get_table_as_dataframe("variable"))
    client.put_variables_from_csv(assets_dir / "existing/variables_new.csv")
    variable = client.get_variable(code_name="TEST1")
    assert variable.name == "Test Variable 1"
    df = client.get_table_as_dataframe("variable")
    assert len(df) == original_len + 2


def test_put_variables_from_dataframe(
    temp_database_path: Path,
):
    df = pd.DataFrame(
        data={
            "name": ["Test Variable 1", "Test Variable 2"],
            "code_name": ["TEST1", "TEST2"],
            "kind": ["TEST", "TEST"],
            "units": ["big_ones", "little_ones"],
        }
    )
    client = csdb.CalSimDatabaseClient(temp_database_path, fill_vars_if_new=False)
    original_len = len(client.get_table_as_dataframe("variable"))
    client.put_variables_from_dataframe(df)
    variable = client.get_variable(code_name="TEST1")
    assert variable.name == "Test Variable 1"
    df = client.get_table_as_dataframe("variable")
    assert len(df) == original_len + 2


@pytest.mark.parametrize(
    "db_path, dss",
    [
        ("temp_database_path", "single_timeseries_dss_7"),
        ("temp_database_path", "single_timeseries_dss_6"),
    ],
)
def test_put_single_timeseries_from_dss(
    db_path: Path,
    dss: Path,
    request: pytest.FixtureRequest,
):
    temp_database_path = request.getfixturevalue(db_path)
    single_timeseries_dss = request.getfixturevalue(dss)
    client = csdb.CalSimDatabaseClient(temp_database_path, fill_vars_if_new=False)
    client.put_variable(name="test", code_name="S_OROVL", kind="test", units="test")
    run, vars, df = client.put_run_from_dss(
        src=single_timeseries_dss,
        run_name="TEST",
    )
    assert run.name == "TEST"
    assert len(vars) == 1
    assert df.index.max() == pd.to_datetime("2021-09-30")


@pytest.mark.parametrize(
    "db_path, dss",
    [
        ("temp_database_path", "multi_timeseries_dss_7"),
        ("temp_database_path", "multi_timeseries_dss_6"),
    ],
)
def test_put_mutli_timeseries_from_dss(
    db_path: Path,
    dss: Path,
    request: pytest.FixtureRequest,
):
    temp_database_path = request.getfixturevalue(db_path)
    multi_timeseries_dss = request.getfixturevalue(dss)
    client = csdb.CalSimDatabaseClient(temp_database_path, fill_vars_if_new=False)
    client.put_variable(name="Oroville", code_name="S_OROVL", kind="test", units="test")
    client.put_variable(name="Banks", code_name="C_CAA003", kind="test", units="test")
    client.put_run_from_dss(
        src=multi_timeseries_dss,
        run_name="RUN1",
    )
    run, vars, df = client.get_result_by_run(run_name="RUN1")
    assert run.name == "RUN1"
    assert len(vars) == 2
    assert set(df.columns) == {"S_OROVL", "C_CAA003"}
    assert df.index.max() == pd.to_datetime("2021-09-30")


@pytest.mark.parametrize(
    "db_path, dss_1, dss_2",
    [
        ("temp_database_path", "single_timeseries_dss_7", "single_timeseries_2_dss_7"),
        ("temp_database_path", "single_timeseries_dss_6", "single_timeseries_2_dss_7"),
    ],
)
def test_append_to_existing_run_from_dss(
    db_path: Path,
    dss_1: Path,
    dss_2: Path,
    request: pytest.FixtureRequest,
):
    temp_database_path = request.getfixturevalue(db_path)
    dss_1_path = request.getfixturevalue(dss_1)
    dss_2_path = request.getfixturevalue(dss_2)
    client = csdb.CalSimDatabaseClient(temp_database_path, fill_vars_if_new=False)
    client.put_variable(name="test", code_name="S_OROVL", kind="test", units="test")
    client.put_variable(name="test", code_name="S_OROVL_2", kind="test", units="test")
    run, vars, df = client.put_run_from_dss(src=dss_1_path, run_name="TEST")
    run, vars, df = client.put_run_from_dss(src=dss_2_path, run_name="TEST")
    assert run.name == "TEST"
    assert len(vars) == 2
    assert df.index.max() == pd.to_datetime("2021-09-30")
    assert list(df.columns) == ["S_OROVL", "S_OROVL_2"]


def test_get_single_timeseries(
    single_run_single_result_db: Path,
):
    client = csdb.CalSimDatabaseClient(single_run_single_result_db)
    run, vars, df = client.get_result_by_run(run_name="TEST")
    assert run.name == "TEST"
    assert len(vars) == 1
    assert df.index.max() == pd.to_datetime("2021-09-30")


def test_get_multiple_timeseries_by_run(
    multi_run_multi_result_db: Path,
):
    client = csdb.CalSimDatabaseClient(multi_run_multi_result_db)
    run, vars, df = client.get_result_by_run(run_name="RUN1")
    assert run.name == "RUN1"
    assert len(vars) == 2
    assert set(df.columns) == {"S_OROVL", "C_CAA003"}
    assert df.index.max() == pd.to_datetime("2021-09-30")

    runs, var, df = client.get_result_by_variable(code_name="S_OROVL")
    assert var.code_name == "S_OROVL"
    assert len(runs) == 2
    assert set(df.columns) == {"RUN1", "RUN2"}
    assert df.index.max() == pd.to_datetime("2021-09-30")


def test_get_variable_counts_by_run(
    multi_run_multi_result_db: Path,
):
    client = csdb.CalSimDatabaseClient(multi_run_multi_result_db)
    s = client.get_variable_counts()
    assert isinstance(s, pd.Series)
    assert s.name == "variable_counts"
    assert s.index.name == "run_name"
    assert s["RUN1"] == 2
