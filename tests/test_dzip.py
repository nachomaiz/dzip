# pylint: disable=redefined-outer-name
# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring

import csv
import io
import json
import zipfile
from unittest import mock

import pytest

import pandas as pd
from pandas.io.parsers import TextFileReader

from dzip import dzip
from dzip.dzip import MetadataContainer, _sizeof_fmt


## Meta
def test_meta_from_config(json_dict: dict[str, dict]):
    assert isinstance(dzip.Metadata.from_config(json_dict), dzip.Metadata)


def test_meta_from_spss_meta(spss_meta_mock: MetadataContainer):
    assert isinstance(dzip.Metadata.from_config(spss_meta_mock), dzip.Metadata)


def test_meta_shape(meta_class: dzip.Metadata):
    assert meta_class.shape == (2, 2)


def test_meta_str(meta_class: dzip.Metadata):
    assert str(meta_class) == "Metadata object with dzip object metadata."


## DZipFile
def test_dzipfile_open_str(zip_bytes: io.BytesIO):
    with zip_bytes as z_buf:
        with mock.patch(
            "builtins.open", mock.mock_open(read_data=z_buf.getvalue())
        ) as mock_file:
            with dzip.DZipFile("x.dzip").fp as f:
                assert isinstance(f, io.BytesIO)
            mock_file.assert_called_with("x.dzip", "rb")


def test_dzipfile_open_bytes(zip_bytes: io.BytesIO):
    with dzip.DZipFile(zip_bytes).fp as f:
        assert isinstance(f, io.BytesIO)


def test_dzipfile_zip(mocker_dzip: dzip.DZipFile):
    assert isinstance(mocker_dzip.zip, zipfile.ZipFile)


def test_dzipfile_open_data(mocker_dzip: dzip.DZipFile):
    with mocker_dzip.open_data() as f:
        reader = csv.reader(io.TextIOWrapper(f))
        assert reader.__next__() == ["Col_A", "Col_B"]


def test_dzipfile_open_meta(mocker_dzip: dzip.DZipFile):
    with mocker_dzip.open_meta() as meta:
        assert json.load(meta)["number_rows"] == 2


def test_dzipfile_meta(mocker_dzip: dzip.DZipFile):
    assert isinstance(mocker_dzip.meta(), dzip.Metadata)


def test_dzipfile_to_pandas(mocker_dzip: dzip.DZipFile):
    assert isinstance(mocker_dzip.to_pandas(), pd.DataFrame)


def test_dzipfile_to_pandas_chunks(mocker_dzip: dzip.DZipFile):
    assert isinstance(mocker_dzip.to_pandas(chunksize=1000), TextFileReader)


def test_dzipfile_extract(mocker_dzip: dzip.DZipFile):
    assert isinstance(mocker_dzip.extract(), tuple)


def test_dzipfile_load(mocker_dzip: dzip.DZipFile):
    with mocker_dzip.load() as (data, meta):
        assert isinstance(data, zipfile.ZipExtFile)
        assert isinstance(meta, dzip.Metadata)


def test_dzipfile_str(mocker_dzip: dzip.DZipFile):
    assert str(mocker_dzip) == "DZipFile object of size=130.0B"


## READ DZIP
@pytest.mark.parametrize(
    ["metadataonly", "chunksize", "ret_type"],
    [
        [False, None, pd.DataFrame],
        [True, None, pd.DataFrame],
        [False, 1, TextFileReader],
        [True, 1, pd.DataFrame],
    ],
)
def test_read_dzip(
    zip_bytes: io.BytesIO,
    monkeypatch: pytest.MonkeyPatch,
    metadataonly: bool,
    chunksize: int | None,
    ret_type: type,
):
    with monkeypatch.context() as patch:
        patch.setattr(dzip.DZipFile, "_load_bytes_obj", lambda x, y: zip_bytes)
        data, meta = dzip.read_dzip("x.dzip", metadataonly, chunksize)
        assert isinstance(data, ret_type)
        assert isinstance(meta, dzip.Metadata)

        if metadataonly:
            assert data.shape == (0, 2)  # type: ignore


## SAVING DZIP
@pytest.mark.parametrize(
    "compress",
    [True, False]
)
def test_save_dzip(read_sav_ret: tuple[pd.DataFrame, dzip.Metadata], compress: bool):
    z_buf = io.BytesIO()
    dzip.save_dzip(z_buf, *read_sav_ret, compress=compress)
    assert all(v in str(z_buf.getvalue()) for v in ("data.csv", "meta.json"))


def test_spss_to_dzip(read_sav_ret: tuple[pd.DataFrame, dzip.Metadata]):
    with mock.patch("pyreadstat.read_sav", return_value=read_sav_ret):
        z_buf = io.BytesIO()
        dzip.spss_to_dzip("x.sav", z_buf)
    assert all(v in str(z_buf.getvalue()) for v in ("data.csv", "meta.json"))


@pytest.mark.parametrize(
    ["size", "res"],
    [
        [1, "1.0B"],
        [1e3, "1.0KB"],
        [1e6, "1.0MB"],
        [1e9, "1.0GB"],
        [1e12, "1.0TB"],
        [1e15, "1.0PB"],
        [1e18, "1.0EB"],
        [1e21, "1.0ZB"],
        [1e24, "1.0YB"],
    ],
)
def test_sizeof_fmt(size: int, res: str):
    assert _sizeof_fmt(size) == res


if __name__ == "__main__":
    pytest.main()
