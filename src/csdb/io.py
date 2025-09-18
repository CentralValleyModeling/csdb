import logging
from pathlib import Path
from typing import Any, Collection, Iterator

import hecdss  # for dss 7
import pandas as pd
import yaml
from hecdss.record_type import RecordType
from hecdss.regular_timeseries import RegularTimeSeries
from pydsstools.heclib.dss import HecDss as HecDss_6  # for dss 6

logger = logging.getLogger(__name__)


def load_yaml(src: Path) -> Any:
    """Convieniently use the yaml.safe_load function.

    Parameters
    ----------
    src : Path
        The path to the yaml file

    Returns
    -------
    Any
        The object that is parsed from the file
    """
    src = Path(src)
    with open(src, "rb") as SRC:
        obj = yaml.safe_load(SRC)
    return obj


def get_dss_file_version(src: Path | str) -> tuple[int, str]:
    """Determine the version of the DSS file.

    This function does not depend on a 3rd party library, and doesn't use a published
    HECLIB API (so it might not work in the future...).

    Parameters
    ----------
    src : Path | str
        The DSS file to interrogate

    Returns
    -------
    tuple[int, str]
        A tuple with both the integer file version, and a string of the full file
        version specification

    Examples
    --------
    >>> get_dss_file_version("file.dss")
    6, "6-IY"
    """
    logger.info(f"determining DSS file verison: {src}")
    # This is a hack, this is not guarenteed to be stable in the DSS API.
    # But USACE doesn't give us a good alternative...
    file_version_spec_start = 16
    file_version_spec_size = 4
    with open(src, "rb") as SRC:
        SRC.seek(file_version_spec_start)
        file_version = SRC.read(file_version_spec_size).decode()
    major_version = int(file_version.split("-")[0])
    logger.info(f"{major_version=}, {file_version=}")
    return major_version, file_version


def _iter_load_dss_7(
    src: Path | str,
    paths: Collection[hecdss.DssPath] | None = None,
) -> Iterator[pd.Series]:
    src = Path(src).absolute()
    if paths is None:
        with hecdss.HecDss(str(src)) as DSS:
            catalog = DSS.get_catalog()
            paths = list(catalog.items)

    with hecdss.HecDss(str(src)) as DSS:
        total = len(paths)
        for i, path in enumerate(paths):
            logger.info(f"reading {i+1}/{total}: {path}")
            rts = DSS.get(str(path))
            if isinstance(rts, hecdss.RegularTimeSeries):
                s = pd.Series(
                    data=rts.values,
                    index=rts.times,
                    name=path.B,
                )
                s.index.name = "datetime"
                # the hecdss library converts the DSS timestamps as below:
                # DSS: 1900-10-31 24:00:00
                # Python: 1900-11-01 00:00:00
                # and CalSim has used the DSS convention to mean "the end of October"
                # so we adjust the datetimes by a single day
                idx: pd.DatetimeIndex = s.index  # type: ignore
                s.index = idx - pd.Timedelta(days=1)
                yield s

            else:
                logger.warning(
                    f"Skipping {path}, returned {type(rts)},"
                    + " expected hecdss.RegularTimeSeries"
                )


def _iter_load_dss_6(
    src: Path | str,
    paths: Collection[hecdss.DssPath] | None = None,
) -> Iterator[pd.Series]:
    src = Path(src).absolute()
    DSS: HecDss_6.Open
    if paths is None:
        with HecDss_6.Open(str(src)) as DSS:
            paths = [
                hecdss.DssPath(path=p, recType=RecordType.RegularTimeSeries.value)
                for p in DSS.getPathnameList("/*/*/*/*/*/*/")
            ]

    with HecDss_6.Open(str(src)) as DSS:
        total = len(paths)
        for i, path in enumerate(paths):
            logger.info(f"reading {i+1}/{total}: {path}")
            rts: RegularTimeSeries = DSS.read_ts(
                str(path),
                trim_missing=True,
            )  # type: ignore
            s = pd.Series(
                data=rts.values,
                index=rts.pytimes,  # type: ignore
                name=path.B,
            )
            s = s.dropna()
            s.index.name = "datetime"
            s.index = s.index - pd.Timedelta(days=1)  # type: ignore
            yield s


def load_dss(
    src: Path | str,
    paths: Collection[hecdss.DssPath] | None = None,
) -> pd.DataFrame:
    """Load a DSS file into a pandas.DataFrame.

    The result will be a tidy DataFrame with columns: 'datetime', 'variable', 'value',
    and 'run'. The function works with both DSS 6 and DSS 7.

    Parameters
    ----------
    src : Path
        The DSS source file.
    paths : list[hecdss.DssPath], optional
        The DSS paths that should be read from the file, if not provided all paths will
        be read from the file, by default None

    Returns
    -------
    pd.DataFrame
        A tidy DataFrame of the data read from the file

    Raises
    ------
    NotImplementedError
        Raised when the version of the DSS file isn't supported or cannot be determined
    """
    frames = list()
    major, file_version = get_dss_file_version(src)
    if major == 7:
        for series in _iter_load_dss_7(src, paths):
            frames.append(series)
    elif major == 6:
        for series in _iter_load_dss_6(src, paths):
            frames.append(series)
    else:
        raise NotImplementedError(f"DSS version not supported: {file_version=}")
    df = pd.concat(frames, axis=1)
    df.index.name = "datetime"
    df.index = pd.to_datetime(df.index)
    # convert the dss table into the ledger format we expect
    # datetime   | variable  | value   | run
    # YYYY-MM-DD | "S_OROVL" | XXXX.XX | "XXXX"
    df_ledger = (
        df.melt(
            value_name="value",
            var_name="variable",
            ignore_index=False,
        )
        .dropna()  # NAs exist in the df becasue of date mis-alignment.
        .reset_index()  # Make datetime a regular column
    )
    return df_ledger


def _get_intersecting_catalog_7(
    dss: Path | str,
    b_parts: Collection[str],
) -> list[hecdss.DssPath]:
    with hecdss.HecDss(str(dss)) as DSS:
        catalog = DSS.get_catalog()
        paths: list[hecdss.DssPath] = list(catalog.items)
    paths = [p for p in paths if p.B.lower() in b_parts]
    return paths


def _get_intersecting_catalog_6(
    dss: Path | str,
    b_parts: Collection[str],
) -> list[hecdss.DssPath]:
    DSS: HecDss_6.Open
    with HecDss_6.Open(str(dss)) as DSS:
        paths_with_duplicates = [
            hecdss.DssPath(path=p) for p in DSS.getPathnameList("/*/*/*/*/*/*/")
        ]
    paths = list()
    seen_b = list()
    for p in paths_with_duplicates:
        if (p.B in seen_b) or (p.B.lower() not in b_parts):
            continue
        paths.append(p.path_without_date())
        seen_b.append(p.B)

    return paths


def get_intersecting_catalog(
    dss: Path | str,
    b_parts: Collection[str],
) -> list[hecdss.DssPath]:
    """Find the intersecting DSS Paths by their B-parts.

    Given a list of B-parts, find the paths that exist in a DSS file that have those
    B-parts. Works with both DSS 6 and DSS 7. The paths that are returned don't retain
    their D-parts (dates).

    Parameters
    ----------
    dss : Path
        The DSS file to check.
    b_parts : list[str]
        The list of B-parts to find within the DSS file.

    Returns
    -------
    list[hecdss.DssPath]
        The list of full DSS Paths (with wildcard D-part).

    Raises
    ------
    NotImplementedError
        Raised when the version of the DSS file isn't supported or cannot be determined.
    """
    logger.debug(f"finding a maximun of {len(b_parts)} intersecting paths in {dss=}")
    b_parts = [b.lower() for b in b_parts]
    major, file_version = get_dss_file_version(dss)
    if major == 7:
        found = _get_intersecting_catalog_7(dss, b_parts)
    elif major == 6:
        found = _get_intersecting_catalog_6(dss, b_parts)
    else:
        raise NotImplementedError(f"DSS version not supported: {file_version=}")
    logger.debug(f"found {len(found)} intersecting paths")
    return found
