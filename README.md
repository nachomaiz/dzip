# dzip
A csv and json implementation of labeled data files, meant to replace proprietary spss files.

## Installation
Install with pip

```pip install https://github.com/nachomaiz/dzip.git```


## Features
- Save and load `.dzip` files (renamed `.zip` files) with the following zip file structure:
    - `data.csv` file containing the data in csv format
    - `meta.json` file containing metadata in json format
- Load data into pandas with `dzip.DZipFile.to_pandas`
- Convert SPSS sav data into dzip

## General usage information
To create a dzip file, you will need an SPSS sav file with metadata, or existing data with user-defined metadata.

A dictionary with the following items (or any class with the following attributes) can be used:
- `variable_value_labels: dict[str, dict[str, str]]`
- `column_names_to_labels: dict[str, str]`
- `number_rows: int`
- `number_columns: int`

An `io.BytesIO` object can be used in place of any file path except for when referring to `.sav` (or `.zsav`) files.


## Read dzip files
If you just want to simply get a dataframe and Metadata object from a dzip file, you can use `read_dzip`:
```
import dzip

data, meta = dzip.read_dzip("path/to/file.dzip")
```

You can also load the metadata only, which will instead load an empty dataframe with populated column names and the metadata object.
```
data, meta = dzip.read_dzip("path/to/file.dzip", metadataonly=True)

data.shape
>>> (0,...)  # loads all columns or those specified in usecols keyword argument.

type(meta)
>>> dzip.Metadata
```

To open a dzip file as a context manager, you can use `dzip.DZipFile.load` which yields a tuple of the data and metadata.
```
with dzip.DZipFile("path/to/file.dzip").load() as (data, meta):
    # do stuff
```

## Read dzip file into pandas
To read a dzip data file into a pandas dataframe, create a new instance of `DZipFile` pointing to the dzip file:

```
data = dzip.DZipFile("path/to/file.dzip").to_pandas()
```

if you want to load metadata as well, you can use 

## Convert SPSS data
To convert a `.sav` (or `.zsav`) file into `.dzip`, you can use `spss_to_dzip`:

```
dzip.spss_to_dzip("path/to/file.sav", "path/to/archive.dzip")
```

## Save dzip file
To save a set of data and metadata into a `.dzip` file, you can use `save_dzip`:

```
dzip.save_dzip("path/to/file.dzip", data, meta)
```
