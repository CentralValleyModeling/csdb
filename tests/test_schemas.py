from pathlib import Path

import pytest
from pydantic import ValidationError

import csdb


def test_run_schema_bad_types():
    with pytest.raises(ValidationError):
        csdb.Run(name="RUN", source=Path("BAR"))  # type: ignore
    with pytest.raises(ValidationError):
        csdb.Run(name=None, source="BAR")  # type: ignore


def test_variable_schema_bad_types():
    valid: dict[str, str | Path] = {
        "name": "FOO",
        "code_name": "BAR",
        "kind": "BAZ",
        "units": "BAT",
    }
    for k in valid:
        bad_args = dict(valid)
        bad_args[k] = Path("FOO")
        with pytest.raises(ValidationError):
            _ = csdb.Variable(**bad_args)  # type: ignore


def test_run_schema_str():
    run = csdb.Run(name="A", source="B")
    assert str(run) == "Run(name=A, source=B)"


def test_variable_schema_str():
    run = csdb.Variable(name="A", code_name="B", kind="C", units="D")
    assert str(run) == "Variable(name=A, code_name=B)"
