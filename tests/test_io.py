import contextlib
import os
from pathlib import Path

import pandas as pd
import pytest

import csdb.io


@contextlib.contextmanager
def new_cwd(x: str | Path):
    if isinstance(x, Path):
        x = str(x.absolute())
    d = os.getcwd()
    os.chdir(x)
    try:
        yield
    finally:
        os.chdir(d)


@pytest.mark.parametrize(
    "dss",
    [
        ("single_timeseries_dss_7"),
        ("single_timeseries_dss_6"),
    ],
)
def test_read_single_timeseries_from_dss(
    dss: Path,
    request: pytest.FixtureRequest,
):
    single_timeseries_dss = request.getfixturevalue(dss)
    df = csdb.io.load_dss(single_timeseries_dss)
    assert list(df.columns) == ["datetime", "variable", "value"]
    assert len(df) == 1212
    assert df["datetime"].max() == pd.to_datetime("2021-09-30")
