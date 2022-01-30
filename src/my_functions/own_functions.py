"""
Created on 1 Jan 2022

@author: Joachim Landstrom, joachim.landstrom@fek.uu.se

This is a script with common functions for the download and post-processing
of the download of Refinitiv Eikon data.

"""

#  Copyright (c) 2022. All right reserved.

# IMPORT PACKAGES
import csv
import pathlib as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

import pandas as pd


def create_source_file_stem(prepend=None, middle=None, end=None):
    """
    Enter three arguments and get the stem name of the source file.

    Arguments:
    prepend -- First string of the file name, e.g. anndats.

    middle -- Middle string denoting the report frequency, e.g. FQ or FS.

    end -- The year for the data, e.g. YYYY as in 2021.

    Return:
    A string with the source file's stem, e.g. anndats_fq_
    """

    # fid = (
    #     prepend + "_" + str(rep_freq.lower()) + "_" + str(year) + ".csv"
    # )  # Name of source file
    if not all([prepend, middle, end]):
        fid = f"{prepend}_{str(middle.lower())}_{str(end)}"
    elif prepend is not None:
        if middle is not None:
            fid = f"{prepend}_{str(middle.lower())}"
        elif end is not None:
            fid = f"{prepend}_{str(end).lower()}"
        else:
            fid = f"{prepend}"
    else:
        if not all([middle, end]):
            fid = f"{str(middle.lower())}_{str(end)}"
        elif middle is not None:
            fid = f"{str(middle.lower())}"
        elif end is not None:
            fid = f"{str(end).lower()}"
    return fid


def create_source_path(source_dir, file):
    """
    Enter two arguments and get the path to the source directory.

    Arguments:

    source_dir: The directory name of the source data.

    file: The source file name

    Return: The path to the source files.
    """
    path = proj_path.joinpath(source_dir, file)
    return path


def create_out_file(
    stem, header, encoding="UTF8", newline="", delimiter="\t", **kwargs
):
    """
    Enter two arguments. Delete if file exist and writes a new file incl header.

    Arguments:

    stem: The stem of the file name of the file that will be created

    header: The list with headers to be added to the file.

    Return: As default it returns a new tab-separated csv-file having UTF-8
     encoding and with a header.

    Notes:
    **kwargs is for DictWriter
    Requires "pathlib"
    """
    if pl.Path.exists(stem):
        pl.Path.unlink(stem)
    # Header to output stem?
    with open(stem, "w", encoding=encoding, newline=newline) as fl:
        writer = csv.DictWriter(
            fl, delimiter=delimiter, fieldnames=header, **kwargs
        )
        writer.writeheader()


def save_to_csv_file(
    df,
    file,
    mode="a",
    sep="\t",
    encoding="utf-8",
    index=False,
    header=False,
    **kwargs,
):
    """
    Enter two arguments. Appends Pandas dataframe to tab separated csv-file.

    Arguments:

    df: The Pandas dataframe

    file: The name to which the dataframe is to be appended.

    Return: An appended, as default, a tab-separated, csv-file having UTF-8
    encoding, no index, and no header.

    Notes:
    """

    df.to_csv(
        file,
        mode=mode,
        sep=sep,
        encoding=encoding,
        index=index,
        header=header,
        **kwargs,
    )


def save_to_parquet_file(df, file, compression="snappy", **kwargs):
    """
    Enter three arguments and save a dataframe as an Apache Parquet database.

    Arguments:

    df: The dataframe to be saved.

    file: The file name of the Apache Parquet database.

    compression: The database's compression. Available compressions are
    "snappy", "gzip", "brotli", or none. Default is "snappy"

    Return: Returns an Apache Parquet database with a chosen compression.

    Notes:
    The function requires either "pyarrow", or "fastparquet".
    Requires "pathlib".
    """
    # Drop existing file
    if pl.Path.exists(file):
        pl.Path.unlink(file)
    # Save DB
    df.to_parquet(file, compression=compression, **kwargs)


def read_csv_file(
    file, delimiter="\t", na_values=" ", dtype=str, low_memory=False, **kwargs
):
    """
    Enter a csv-file name (incl path), and it returns a Pandas dataframe.

    Arguments:

    file: The csv-file to be read.

    delimiter: Column delimited. Default is tab.

    na_values: How to mark NAs? Default is to leave empty.

    dtype: Data type. Default is string. Use a dict to specify datatype for
    individual columns.

    low_memory: Default False to reduce risk of mixed type interference. Use
    True if short of memory

    Return: Returns a Pandas dataframe

    Notes:
    The function requires Pandas.
    """
    df = pd.read_csv(
        file,
        delimiter=delimiter,
        na_values=na_values,
        dtype=dtype,
        low_memory=low_memory,
        **kwargs,
    )
    return df


def clean_data(in_df, idx, col_vars):
    """
    Reads a Pandas dataframe, drops missing observations and returns it again.

    This function use a two-step approach to drop NaN. In the first pass it
    looks into a list of columns and drops NaN of and drops row with NaN in any
    of the specified columns. The second pass it looks into another list of
    columns and drops rows having NaN in all those columns. Finally, it also
    checks for duplicates in the first list of columns (aka the index columns),
    and keeps only the first observation.

    Arguments:

    in_df: The Pandas dataframe to be cleaned.

    idx: A list of column(s) [index column(s)]. Any NaN in any col lead to drop
    of the row. Used in the first pass.

    col_vars: A list of column(s). NaN in all there cols lead to drop of the
    row. This is used in the second pass
    """

    # Drop if NaN in any of the index columns
    df = in_df.dropna(how="any", subset=idx)
    # Drop if NaN in all variable columns
    df = df.dropna(how="all", subset=col_vars)
    # Drop of there exists duplicates
    idx_all = idx + col_vars
    df = df.sort_values(by=idx_all, na_position="last")
    df = df.drop_duplicates(keep="first")
    return df
