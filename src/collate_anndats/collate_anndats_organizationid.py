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

The key in the announcement dates files is RIC, but RIC is per financial
instrument, whereas the announcement dates are per firm. This script
therefore replaces the RIC key with the Organization's Permanent ID.

"""

__updated__ = "2021-12-06 10:00:00"

#  Copyright (c) 2021. All right reserved.

# IMPORT PACKAGES
import csv
import pathlib as pl

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
    with open(stem, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=header)
        writer.writeheader()


def save_to_csv_file(df, file, **kwargs):  # TODO Add kwargs
    """
    Enter two arguments. Appends Pandas dataframe to tab separated csv-file.

    Arguments:

    df: The Pandas dataframe

    file: The name to which the dataframe is to be appended.

    Return: An appended, tab-separated, csv-file having UTF-8 encoding,
    no index, and no header.

    Notes:
    **kwargs not yet implemented.
    """

    df.to_csv(
        file,
        mode="a",
        sep="\t",
        encoding="utf-8",
        index=False,
        header=False,
        )


def save_to_parquet_file(df, file, compression, **kwargs):  # TODO Add kwarg
    """
    Enter three arguments and save a dataframe as an Apache Parquet database.

    Arguments:

    df: The dataframe to be saved.

    file: The file name of the Apache Parquet database.

    compression: The database's compression. Available compressions are
    ‘snappy’, ‘gzip’, ‘brotli’, or none.

    Return: Returns an Apache Parquet database with a speficic compression.

    Notes:
    **kwargs not yet implemented.
    The function requires either 'pyarrow', or 'fastparquet'.
    """
    # Drop existing file
    if pl.Path.exists(file):
        pl.Path.unlink(file)
    # Save DB
    df.to_parquet(file, compression=compression)


def read_csv_file(file):
    """Reads a tab-delimited csv-file and returns a Pandas dataframe."""
    df = pd.read_csv(file, delimiter="\t", na_values=" ", low_memory=False)
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

    # Drop if NaN in and of the index columns
    df = in_df.dropna(how="any", subset=idx)
    # Drop if NaN in all variable columns
    df = df.dropna(how="all", subset=col_vars)
    # Drop of there exists duplicates of the index columns
    df = df.drop_duplicates(subset=idx, keep="first")
    return df


if __name__ == "__main__":
    # YEAR RANGE
    first_year: int = 2000
    last_year: int = 2021

    # PROJECT DIRECTORY
    proj_path = pl.Path.home().joinpath("Documents", "research", "eikon")

    # NAME OF SOURCE AND OUTPUT DIRECTORY (INSIDE PROJECT DIRECTORY FROM ABOVE)
    SOURCE_DIR = "raw2"
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
    # in_freq = ["fq", "fq"]
    in_freq = ["fq"]

    # PROCESS START
    print("Collating announcement dates into a single file")
    for my_year in range(first_year, last_year + 1):
        print("   - Year: " + str(my_year))
        for report_type in in_freq:
            # file name convention anndats_act_fs_2005.csv
            SOURCE_FILE_NAME = create_source_file_stem(
                FILE_PREFIX, report_type, my_year
                )
            source_path = create_source_path(SOURCE_DIR, SOURCE_FILE_NAME)
            dta = read_csv_file(source_path)
            my_header = list(dta.columns.values)
            my_idx = list(my_header[0:2])
            my_vars = list(my_header[2:7])
            # Parse PeriodEndDate into datetime object
            dta[my_header[1:2]] = pd.to_datetime(dta[my_header[1:2]], utc=False)
            for ts in my_vars:
                # Parse timestamp into datetime object, and standardize tz to UTC
                dta[ts] = pd.to_datetime(dta[ts], utc=True)
                # Convert timestamp to tz 'CET'
                dta[ts] = dta[ts].dt.tz_convert("CET")
            dta = dta.sort_values(by=my_idx)
            dta = clean_data(dta, my_idx, my_vars)
            # save_to_csv_file(dta, out_path)
            if my_year == 2000 and report_type == "fs":
                # Delete any existing file and save new
                save_to_parquet_file(dta, out_path, compression="snappy")
            else:
                # Append data to file
                old_dta = pd.read_parquet(out_path)
                dta = old_dta.append(dta)
                save_to_parquet_file(dta, out_path, compression="snappy")

    print("Finalizing raw dataset")
    dta = pd.read_parquet(out_path)
    my_idx = list(my_header[0:2])
    my_vars = list(my_header[2:7])
    dta = dta.sort_values(by=my_idx)
    dta = clean_data(dta, my_idx, my_vars)
    save_to_parquet_file(dta, out_path, compression="snappy")

    # Read in data
    print("Post-processing the raw dataset into a final dataset")
    dta = pd.read_parquet(out_path)
    # Sort
    dta = dta.sort_values(by=my_idx)
    # Drop duplicates
    dta = dta.drop_duplicates(
        subset=[
            "OrganizationID",
            "PeriodEndDate",
            "OriginalAnnouncementDate",
            ],
        keep="first",
        )
    # # Set outfile names and save
    # my_header = list(dta.columns.values)
    # out_file_name2 = "OrganizationID_cleaned.csv"  # Name of out file
    # out_path2 = proj_path.joinpath("out", out_file_name2)
    # create_out_file(out_path2, ["OrganizationID"])
    # id_df = id_df.sort_values(by=["OrganizationID"])
    # save_to_csv_file(id_df["OrganizationID"], out_path2)

    save_to_parquet_file(dta, out_path_final, compression="brotli")
    print("File " + "--" + str(out_path_final) + "--" + " is saved.")
    print("It has " + str(len(dta)) + " rows, and has the following header:")
    print(my_header)
    print("Done")
