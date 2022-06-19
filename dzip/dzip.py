from contextlib import contextmanager
from dataclasses import dataclass
import io
import json
from typing import IO, Generator, Protocol, overload
import zipfile

import pandas as pd
from pandas.io.parsers import TextFileReader
import pyreadstat


class MetadataContainer(Protocol):
    variable_value_labels: dict[str, dict[str, str]]
    column_names_to_labels: dict[str, str]
    number_rows: int
    number_columns: int


@dataclass()
class Metadata:
    variable_value_labels: dict[str, dict[str, str]]
    column_names_to_labels: dict[str, str]
    number_rows: int
    number_columns: int

    @classmethod
    def from_config(cls, config: dict | MetadataContainer):
        if isinstance(config, dict):
            return cls(**config)
        return cls(
            config.variable_value_labels,
            config.column_names_to_labels,
            config.number_rows,
            config.number_columns,
        )

    @property
    def shape(self) -> tuple[int, int]:
        return self.number_rows, self.number_columns

    def __str__(self):
        return f"{self.__class__.__name__} object with dzip object metadata."


class DZipFile:
    engine = zipfile.ZipFile

    def __init__(self, file: str | io.BytesIO) -> None:
        # TODO: implement more ZipFile-like read/write functionality
        if isinstance(file, str):
            self.fp = self._load_bytes_obj(file)
        else:
            self.fp = file

    @staticmethod
    def _load_bytes_obj(file: str) -> io.BytesIO:
        with open(file, "rb") as f:
            bytes_content = f.read()
            return io.BytesIO(bytes_content)

    @property
    def zip(self) -> zipfile.ZipFile:
        return self.engine(self.fp)

    @contextmanager
    def open_data(self) -> Generator[IO[bytes], None, None]:
        file = self.zip
        try:
            yield file.open("data.csv", "r")
        finally:
            file.close()

    @contextmanager
    def open_meta(self) -> Generator[IO[bytes], None, None]:
        file = self.zip
        try:
            yield file.open("meta.json")
        finally:
            file.close()

    def meta(self) -> Metadata:
        with self.open_meta() as meta:
            return Metadata(**json.load(meta))

    @overload
    def to_pandas(self, chunksize: None = ..., **kwargs) -> pd.DataFrame:...
    @overload
    def to_pandas(self, chunksize: int = ..., **kwargs) -> TextFileReader:...

    def to_pandas(
        self, chunksize: int | None = None, **kwargs
    ) -> pd.DataFrame | TextFileReader:
        """Convenience method for loading data by running `pandas.read_csv`. Returns DataFrame or TextFileReader object for chunking."""

        with self.open_data() as f:
            return pd.read_csv(f, chunksize=chunksize, **kwargs)

    def extract(self) -> tuple[pd.DataFrame, Metadata]:
        """Load file into a pandas DataFrame and a Metadata dataclass."""
        return self.to_pandas(chunksize=None), self.meta()

    @contextmanager
    def load(self) -> Generator[tuple[IO[bytes], Metadata], None, None]:
        file = self.zip

        try:
            data = file.open("data.csv", "r")
            meta = Metadata(**json.load(file.open("meta.json")))

            yield data, meta
        finally:
            file.close()

    def __str__(self) -> str:
        size = _sizeof_fmt(sum(zinfo.compress_size for zinfo in self.zip.filelist))

        return f"{self.__class__.__name__} object of size={size}"


def save_dzip(
    file: str | io.BytesIO, data: pd.DataFrame, meta: Metadata, compress: bool = True
) -> None:
    compression = zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED

    with zipfile.ZipFile(file, "w", compression) as zf:
        with zf.open("data.csv", "w") as f:
            data.to_csv(f, index=False)

        with zf.open("meta.json", "w") as f:
            json_bytes = json.dumps(meta.__dict__, ensure_ascii=False).encode("utf-8")
            f.write(json_bytes)


def spss_to_dzip(sav_filepath: str, file: str | io.BytesIO) -> None:
    out = pyreadstat.read_sav(sav_filepath)
    data: pd.DataFrame = out[0]
    meta = Metadata.from_config(out[1])

    save_dzip(file, data, meta)


@overload
def read_dzip(
    file: str | io.BytesIO,
    metadataonly: bool = False,
    chunksize: None = ...,
    **kwargs,
) -> tuple[pd.DataFrame, Metadata]:...
@overload
def read_dzip(
    file: str | io.BytesIO,
    metadataonly: bool = False,
    chunksize: int = ...,
    **kwargs,
) -> tuple[TextFileReader, Metadata]:...


def read_dzip(
    file: str | io.BytesIO,
    metadataonly: bool = False,
    chunksize: int | None = None,
    **kwargs,
) -> tuple[pd.DataFrame | TextFileReader, Metadata]:

    dzfile = DZipFile(file)

    meta = dzfile.meta()

    if metadataonly:
        return (
            pd.DataFrame(None, columns=list(meta.column_names_to_labels.keys())),
            meta,
        )

    return dzfile.to_pandas(chunksize, **kwargs), meta


def _sizeof_fmt(num, suffix="B"):
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1000.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1000.0
    return f"{num:.1f}Y{suffix}"
