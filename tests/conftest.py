import json
import tempfile
from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture(scope="session")
def assets_dir() -> Path:
    return Path(__file__).parent / "assets"


@pytest.fixture(scope="function")
def temp_database_path(assets_dir: Path) -> Generator[Path, None, None]:
    created_dir = assets_dir / "created"
    with tempfile.TemporaryDirectory(dir=created_dir) as DIR:
        p = Path(DIR) / "temp.db"
        yield p


@pytest.fixture(scope="session")
def calculated_values(assets_dir: Path) -> dict[str, list]:
    f = assets_dir / "existing/calculated_values.json"
    with open(f, "r") as F:
        obj = json.load(F)
    return obj


@pytest.fixture(scope="session")
def single_timeseries_dss_7(assets_dir: Path) -> Path:
    return assets_dir / "existing/single_timeseries.hec7.dss"


@pytest.fixture(scope="session")
def single_timeseries_2_dss_7(assets_dir: Path) -> Path:
    return assets_dir / "existing/single_timeseries.2.hec7.dss"


@pytest.fixture(scope="session")
def multi_timeseries_dss_7(assets_dir: Path) -> Path:
    return assets_dir / "existing/multi_timeseries.hec7.dss"


@pytest.fixture(scope="session")
def single_timeseries_dss_6(assets_dir: Path) -> Path:
    return assets_dir / "existing/single_timeseries.hec6.dss"


@pytest.fixture(scope="session")
def single_timeseries_2_dss_6(assets_dir: Path) -> Path:
    return assets_dir / "existing/single_timeseries.2.hec6.dss"


@pytest.fixture(scope="session")
def multi_timeseries_dss_6(assets_dir: Path) -> Path:
    return assets_dir / "existing/multi_timeseries.hec6.dss"


@pytest.fixture(scope="session")
def single_run_single_result_db(assets_dir: Path) -> Path:
    return assets_dir / "existing/single_run_single_result.db"


@pytest.fixture(scope="session")
def multi_run_multi_result_db(assets_dir: Path) -> Path:
    return assets_dir / "existing/multi_run_multi_result.db"


@pytest.fixture(scope="session")
def full_study(assets_dir: Path) -> Path:
    return assets_dir / "existing/full_study.db"
