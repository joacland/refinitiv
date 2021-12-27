"""
Created on 5 Dec 2021

@author: Joachim Landstrom, joachim.landstrom@fek.uu.se

This script reads an ods file with data collected from Refinitiv's
Advanced Filing Search with dates for publication of annual report for
Swedish firms.

"""
import pathlib as pl
import pandas as pd
from pandas_ods_reader import read_ods

pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)


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


def save_to_csv_file(df, file, **kwargs):
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
        **kwargs
    )


def save_to_parquet_file(df, file, compression, **kwargs):
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
    df.to_parquet(file, compression=compression, **kwargs)


def read_csv_file(file, **kwargs):
    """Reads a tab-delimited csv-file and returns a Pandas dataframe."""
    df = pd.read_csv(
        file, delimiter="\t", na_values=" ", low_memory=False, **kwargs
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
    # PROJECT DIRECTORY
    proj_path = pl.Path.home().joinpath("Documents", "research", "refinitiv")

    # NAME OF SOURCE AND OUTPUT DIRECTORY (INSIDE PROJECT DIRECTORY FROM ABOVE)
    SOURCE_DIR = "raw"
    OUT_DIR = "out"

    # RAW FILE NAME
    RAW_FILE = "anndats_ar_eikon_stripped.ods"
    raw_path = proj_path.joinpath(SOURCE_DIR, RAW_FILE)  # Path to raw data file

    # OrganizationID-CommonName FILE NAME
    ORGID_CONM_FILE = "commonname_v2.csv"
    orgid_conm_path = proj_path.joinpath(SOURCE_DIR, ORGID_CONM_FILE)

    # RIC-OrganizationID FILE NAME
    ORGID_RIC_FILE = "ric_organizationid.csv"
    orgid_ric_path = proj_path.joinpath(SOURCE_DIR, ORGID_RIC_FILE)

    # OUT FILE NAME PREFIX
    FILE_PREFIX = "anndats_annrep_refinitiv"

    # FILE NAMES OF DATA (STATA format)
    OUT_FILE_NAME = FILE_PREFIX + ".dta"  # Name of out file
    out_path = proj_path.joinpath(
        OUT_DIR, OUT_FILE_NAME
    )  # Out path for output file

    # Date format for the Filing Date and Document Date
    D_FORMAT = "%Y-%m-%d"  # E.g. 2020-12-31

    # Read ODS file
    df = read_ods(raw_path)
    my_header = list(df.columns.values)
    # Drop if mod == 999 (not publicly traded)
    # or 9 (cannot find external evidence (in press release, in invitation to
    # annual shareholders' meeting, or, sometimes, in either press release with
    # financial calendar or in the Q4 report).
    df = df[df["mod"] != 999]
    df = df[df["mod"] != 9]

    df = df[
        [
            "Receipt Date",
            "Filing Date",
            "Document Date",
            "Company Name",
            "RIC",
            "Size",
            "DCN",
        ]
    ]
    df = df.rename(
        columns={
            "Receipt Date": "ReceiptDate",
            "Filing Date": "anndats_act",
            "Document Date": "datadate",
        }
    )
    # Convert datetime strings to Pandas datetime
    df["ReceiptDate"] = pd.to_datetime(df["ReceiptDate"])
    for date in ["anndats_act", "datadate"]:
        df[date] = pd.to_datetime(df[date], format=D_FORMAT)
        # Strip the time from the datetime
        # df[date] = df[date].dt.date

    my_header = list(df.columns.values)

    # Read OrganizationID files
    # orgid_conm = read_csv_file(orgid_conm_path, dtype=str)
    # orgid_conm = orgid_conm.drop_duplicates()
    # orgid_conm = orgid_conm.dropna(how="any")

    orgid_ric = read_csv_file(orgid_ric_path, dtype=str)

    # Find IPO RICs and strip the IPO- prefix, and append to original
    ipo = orgid_ric[orgid_ric.RIC.str.contains("^IPO-")]
    ipo_original = ipo["RIC"].str.replace(
        r"^IPO-", "", regex=True, case=True
    )
    ipo_original = ipo_original.to_frame()
    ipo_original = ipo_original.drop_duplicates()
    orgid_ric = orgid_ric.append(ipo_original)
    orgid_ric = orgid_ric.drop_duplicates()

    # Find RIC-series delisted, strip delisting suffix, and append to original
    delisted = orgid_ric[orgid_ric.RIC.str.contains("\^[A-Z][0-9]{2}")]
    delisted_original = delisted["RIC"].str.replace(
        r"\^[A-Z][0-9]{2}", "", regex=True, case=True
    )
    delisted_original = delisted_original.to_frame()
    delisted_original = delisted_original.drop_duplicates()
    orgid_ric = orgid_ric.append(delisted_original)
    orgid_ric = orgid_ric.drop_duplicates()
    orgid_ric = orgid_ric.dropna(how="any")

    df = df.merge(orgid_ric, how="left", on="RIC")
    # df_missing = df[df["OrganizationID"].isna()]
    # print(df_missing[["Company Name", "Document Date", "RIC"]])

    df = df[
        [
            "OrganizationID",
            "datadate",
            "anndats_act",
        ]
    ]
    df = df.drop_duplicates()
    df = df.dropna(how="any")

    my_header = list(df.columns.values)
    print("It has " + str(len(df)) + " rows, and has the following header:")
    print(my_header)
    df.to_stata(
        out_path,
        write_index=False,
        data_label="Annual Report Announcement Dates from Refinitiv Eikon",
        convert_dates={
            "datadate":"td",
            "anndats_act":"td"
        },
        variable_labels={
            "OrganizationID": "TR.OrganizationID in Refinitiv",
            "anndats_act": "Announcement Date",
            "datadate": "Fiscal Period End Date"
        },
        version=119,
    )
