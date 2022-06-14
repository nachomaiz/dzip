# pylint: disable=redefined-outer-name
# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring

from contextlib import contextmanager
import csv
from dataclasses import dataclass
import io
import json
from typing import Any, Callable, Generator
import zipfile

import pytest

import pandas as pd

from dzip import dzip


test_csv = [["Col_A", "Col_B"], ["a_1", "b_1"], ["a_2", "b_2"]]


@pytest.fixture
def csv_file() -> list[list[str]]:
    return test_csv


test_meta = {
    "variable_value_labels": {"Col_A": {"a_1": "A"}, "Col_B": {"b_1": "B"}},
    "column_names_to_labels": {"Col_A": "a", "Col_B": "a"},
    "number_rows": 2,
    "number_columns": 2,
}

@pytest.fixture
def dataframe() -> pd.DataFrame:
    return pd.DataFrame(test_csv[1:], columns=test_csv[0])

@pytest.fixture
def json_dict() -> dict[str, Any]:
    return test_meta


@pytest.fixture
def meta_class() -> dzip.Metadata:
    return dzip.Metadata(**test_meta)


@dataclass(slots=True)
class MockSPSSMeta:
    variable_value_labels: dict[str, dict[str, str]]
    column_names_to_labels: dict[str, str]
    number_rows: int
    number_columns: int


@pytest.fixture
def spss_meta_mock() -> MockSPSSMeta:
    return MockSPSSMeta(**test_meta)


@contextmanager
def generate_zip_bytes() -> Generator[io.BytesIO, None, None]:
    z = io.BytesIO()
    zf = zipfile.ZipFile(z, "a", zipfile.ZIP_DEFLATED, False)
    try:
        result = io.StringIO()
        writer = csv.writer(result)
        writer.writerows(test_csv)

        zf.writestr("data.csv", result.getvalue(), zipfile.ZIP_DEFLATED)
        zf.writestr("meta.json", json.dumps(test_meta), zipfile.ZIP_DEFLATED)

        yield z

    finally:
        zf.close()


@pytest.fixture
def zip_bytes() -> io.BytesIO:
    with generate_zip_bytes() as b:
        ret = b

    return ret


@pytest.fixture
def zip_file() -> zipfile.ZipFile:
    with generate_zip_bytes() as z_buf:
        z = z_buf

    return zipfile.ZipFile(z, "r", zipfile.ZIP_DEFLATED, False)


@pytest.fixture
def mocker_dzip(
    zip_bytes: io.BytesIO, monkeypatch: pytest.MonkeyPatch
) -> dzip.DZipFile:
    with monkeypatch.context() as patch:
        patch.setattr(dzip.DZipFile, "_load_bytes_obj", lambda x, y: zip_bytes)
        return dzip.DZipFile("x.dzip")

@pytest.fixture
def none_func() -> Callable[...,None]:
    def mocker_func(*args, **kwargs):
        return
    return mocker_func

@pytest.fixture
def read_sav_ret(dataframe: pd.DataFrame, meta_class: dzip.Metadata) -> tuple[pd.DataFrame, dzip.Metadata]:
    return dataframe, meta_class

# @pytest.fixture
# def read_sav_ret(dataframe: pd.DataFrame, meta_class: dzip.Meta) -> Callable[...,tuple[pd.DataFrame, dzip.Meta]]:
#     def mocker_func(*args, **kwargs):
#         return dataframe, meta_class
    
#     return mocker_func

# @pytest.fixture
# def mocker_zip(monkeypatch: pytest.MonkeyPatch) -> zipfile.ZipFile:
#     with monkeypatch.context() as patch:
#         patch.setattr(zipfile.ZipExtFile, "open", io.BytesIO)
#       return zipfile.ZipFile
