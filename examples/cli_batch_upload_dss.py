import argparse
import logging
import sys
from pathlib import Path

from csdb import Client

logger = logging.getLogger(__name__)


def cli(args: list[str] | None = None):
    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="CalSim Database Batch Upload",
        description="Batch upload CalSim results from DSS files to a CalSim database.",
    )
    parser.add_argument(
        "src",
        type=Path,
        help="""The directory full of DSS files to read (does not look in sub-folders)
        """,
    )
    parser.add_argument(
        "db",
        type=Path,
        help="""The database to connect to (will be created if it doesn't exist)
        """,
    )
    parser.add_argument(
        "vars",
        type=Path,
        help="""The variables to initialize the database with if it needs to be created.
        """,
    )

    namepspace = parser.parse_args(args)
    src: Path = namepspace.src
    db: Path = namepspace.db
    vars: Path = namepspace.vars

    files = sorted([f for f in src.iterdir() if f.suffix == ".dss"])
    batch_upload_from_dss(db, files, vars)


def batch_upload_from_dss(
    database: Path,
    files: list[Path],
    vars: Path | bool = True,
):
    client = Client(database, fill_vars_if_new=vars)
    for f in files:
        logger.info(f"Uploading '{f.stem}': {f}")
        client.put_run_from_dss(f.stem, f)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cli()
