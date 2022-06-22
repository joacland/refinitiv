"""
Created on 5 Dec 2021

@author: Joachim Landstrom, joachim.landstrom@fek.uu.se

This script reads a set of csv with announcement dates, collates them
and removes NaN and duplicates.

It also adjusts the time zone for the
timestamps to be 'CET'. Data is saved into an Apache Parquet file with
compression set to be 'brotli'.

File naming convention for the raw csv files is prefix + _
+ report frequency + _ + fiscal year + suffix as in e.g.
anndats_act_fs_2005.csv, where fs can be either {'fs', 'fq'}. 'fs' is
interim reporting frequency semi-annually, whereas 'fq' is quarterly
frequency. 2005 is the year. The range of years are set with 'first_year'
and 'last_year'.

Any deviation of the naming convention for the raw csv-files renders adj
to the create_source_file_stem function.

The key in the announcement dates files is RIC, but RIC is per QuoteID,
whereas the announcement dates are per firm. This script
therefore replaces the RIC key with the Organization's Permanent ID.

"""

#  Copyright (c) 2021. All right reserved.

# IMPORT PACKAGES
import csv
import pathlib as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

import pandas as pd


# import sys


def create_source_file_stem(prepend, rep_freq, year):
    """
    Enter three arguments and get the stem name of the source file.

    Arguments:
    prepend -- First string of the file name (e.g. anndats_act).

    rep_freq -- Middle string denoting the report frequency. Eg. FQ or FS.

    year -- The year for the data. Eg. YYYY as in 2021.

    Return:
    A string with the source file's stem
    """

    fid = (
        prepend + "_" + str(rep_freq.lower()) + "_" + str(year) + ".csv"
    )  # Name of source file
    return fid


def create_source_path(s_dir, file):
    """
    Enter two arguments and get the path to the source directory.

    Arguments:

    directory: The directory name of the source data.

    file: The source file name

    Return: The path to the source files.
    """
    path = proj_path.joinpath(s_dir, file)
    return path


def create_out_file(stem, header, **kwargs):
    """
    Enter two arguments. Delete if file exist and save new file incl header.

    Arguments:

    stem: The stem of the file name of the file that will be created

    header: The list with headers to be added to the file.

    Return: A new tab-separated csv-file having UTF-8 encoding and with a header.

    Notes:
    **kwargs not yet implemented.
    """
    if pl.Path.exists(stem):
        pl.Path.unlink(stem)
    # Header to output stem?
    with open(stem, "w", encoding="UTF8", newline="") as fl:
        writer = csv.DictWriter(fl, delimiter="\t", fieldnames=header)
        writer.writeheader()


def save_to_csv_file(
    df,
    file,
    mode="a",
    sep="\t",
    encoding="utf-8",
    index=False,
    header=False,
    **kwargs
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
        **kwargs
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
    "Pathlib" is also needed.
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
    Reads, as default, a tab-delimited csv-file and returns a Pandas dataframe.
    """
    df = pd.read_csv(
        file,
        delimiter=delimiter,
        na_values=na_values,
        dtype=dtype,
        low_memory=low_memory,
        **kwargs
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

    idx: A list of column(s) (index columns). Any NaN in any col lead to drop
    of the row.

    col_vars: A list of column(s). NaN in all there cols lead to drop of the row.
    """

    # Drop if NaN in any of the index columns
    # print(f"Row pre cleaning are {len(in_df)}")
    df = in_df.dropna(how="any", subset=idx)
    # print(f" -No na idx, rows remain: {len(df)}")
    # df = in_df.dropna(how="any", subset=idx[1:2])
    # print(f" -No na idx[1], rows remain: {len(df)}")
    # Drop if NaN in all variable columns
    df = df.dropna(how="all", subset=col_vars)
    # print(f" -No na other, rows remain: {len(df)}")

    # Drop of there exists duplicates of the index columns

    # df = df.drop_duplicates(subset=idx, keep="first")
    # Drop of there exists duplicates
    idx_all = idx + col_vars
    df = df.sort_values(by=idx_all, na_position="last")
    df = df.drop_duplicates(keep="first")
    # print(f" -No dups, rows remain: {len(df)}")
    return df


if __name__ == "__main__":
    # YEAR RANGE
    first_year: int = 2000
    last_year: int = 2022

    # PROJECT DIRECTORY
    proj_path = pl.Path.home().joinpath("Documents", "research", "refinitiv")

    # NAME OF SOURCE AND OUTPUT DIRECTORY (INSIDE PROJECT DIRECTORY FROM ABOVE)
    SOURCE_DIR = "raw"
    OUT_DIR = "out"

    # FILE NAME PREFIX
    FILE_PREFIX = "anndats_act"

    # FILE NAMES OF DATA
    OUT_FILE_NAME = FILE_PREFIX + "_raw.parquet.snappy"  # Name of raw out file
    out_path = proj_path.joinpath(
        OUT_DIR, OUT_FILE_NAME
    )  # Out path for raw out

    OUT_FILE_NAME_FINAL = (
        FILE_PREFIX + "_OrganizationID.parquet.brotli"
    )  # Name of final out file
    out_path_final = proj_path.joinpath(
        OUT_DIR, OUT_FILE_NAME_FINAL
    )  # Out path for final data

    # INTERIM FREQUENCY
    in_freq = ["fq", "fs"]
    freq_cnt = 0
    # in_freq=["fq"]

    # Date format for the PeriodEndDate (column 2)
    D_FORMAT = "%Y-%m-%d"  # E.g. 2020-12-31

    # PROCESS START
    print("Collating announcement dates into a single file")
    for my_year in range(first_year, last_year + 1):
        print("   - Year: " + str(my_year))
        for report_type in in_freq:
            freq_cnt += 1
            # file name convention anndats_act_fs_2005.csv
            source_file_name = create_source_file_stem(
                FILE_PREFIX, report_type, my_year
            )
            source_path = create_source_path(SOURCE_DIR, source_file_name)
            dta = read_csv_file(source_path, parse_dates=["PeriodEndDate"])
            # Add column with report frequency (Q)uarterly or (S)emi-annual
            if report_type == "fs":
                dta["rp"] = "S"  # Semi-annual report frequency
            else:
                dta["rp"] = "Q"  # Quarterly report freq is default

            # print(list(dta.columns))
            my_header = list(dta.columns.values)
            my_idx = list(my_header[0:2])
            my_vars = list(my_header[2:7])
            # print(f"Reading file {source_path}, which has {len(dta)} rows.")
            # Parse PeriodEndDate into datetime object (now obsolete)
            # dta[my_idx[1]] = pd.to_datetime(dta[my_idx[1]], format=D_FORMAT)
            for ts in my_vars:
                # Parse timestamp into datetime object, and standardize tz to UTC
                dta[ts] = pd.to_datetime(dta[ts], utc=True)
                # Convert timestamp to tz 'CET'
                dta[ts] = dta[ts].dt.tz_convert("CET")
            dta = dta.sort_values(by=my_idx)
            # print(f"   -File has {len(dta)} rows.")
            dta = clean_data(dta, my_idx, my_vars)
            # print(list(dta.columns))
            # print(f"   --Cleaned file has {len(dta)} rows.")
            # save_to_csv_file(dta, out_path)
            if my_year == first_year and freq_cnt == 1:
                # Delete any existing file and save new
                save_to_parquet_file(dta, out_path, compression="snappy")
            else:
                # Append data to file
                old_dta = pd.read_parquet(out_path)
                dta = old_dta.append(dta)
                # print(f"   --Old data has {len(old_dta)} rows while new dta has {len(dta)} rows.")
                # print(list(dta.columns))
                save_to_parquet_file(dta, out_path, compression="snappy")

    print("Finalizing raw dataset")
    dta = pd.read_parquet(out_path)
    # print(dta.dtypes)
    my_idx = list(my_header[0:2])
    my_vars = list(my_header[2:7])
    dta = dta.sort_values(by=my_idx)
    dta = clean_data(dta, my_idx, my_vars)

    # Strip timzone from timestamps. Timestamps are already set to CET.
    # Stata can't interpret Python timestamps with time zone info.
    for ts in my_vars:
        dta[ts] = dta[ts].apply(lambda x: datetime.replace(x, tzinfo=None))

    save_to_parquet_file(dta, out_path, compression="snappy")

    # Read in data
    print("Post-processing the raw dataset into a final dataset")
    dta = pd.read_parquet(out_path)
    # Sort
    dta = dta.sort_values(by=my_header, na_position="last")
    # Drop duplicates
    dta = dta.drop_duplicates(
        subset=[
            "OrganizationID",
            "PeriodEndDate",
            "OriginalAnnouncementDate",
        ],
        keep="first",
    )

    # Set outfile names and save
    # my_header = list(dta.columns.values)
    # out_file_name2 = "test.csv"  # Name of out file
    # out_path2 = proj_path.joinpath("out", out_file_name2)
    # create_out_file(out_path2, my_header)
    # dta = dta.sort_values(
    #     by=["OrganizationID", "PeriodEndDate", "OriginalAnnouncementDate"]
    # )
    # save_to_csv_file(dta, out_path2)

    save_to_parquet_file(dta, out_path_final, compression="brotli")
    out_path = proj_path.joinpath("out", "anndats_act_OrganizationID.dta")
    dta.to_stata(
        out_path,
        write_index=False,
        data_label="Refinitiv's information about interim announcement dates",
        variable_labels={
            "OrganizationID": "TR.OrganizationID in Refinitiv",
            "PeriodEndDate": "TR.F.PeriodEndDate (Fiscal Period End Date)",
            "OriginalAnnouncementDate": "TR.OriginalAnnouncementDate",
            "EPSActReportDate": "TR.EPSActReportDate (IBES report date for EPS [main var])",
            "EPSFRActReportDate": "TR.EPSFRActReportDate (IBES report date for EPS Reported [not main var])",
            "EBITActReportDate": "TR.EBITActReportDate. IBES report date for EBIT [not main var]",
            "EBITDAActReportDate": "TR.EBITDAActReportDate.IBES report date for EBITDA [not main var]",
            "rp": "Report frequency. (Q)uarterly/(S)emi-annual",
        },
        convert_dates={
            "PeriodEndDate": "td"
        },
        version=119,
    )
    print("File " + "--" + str(out_path_final) + "--" + " is saved.")
    print("It has " + str(len(dta)) + " rows, and has the following header:")
    print(my_header)
    print("Done")
