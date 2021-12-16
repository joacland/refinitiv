"""
Created on 6 Dec 2021

@author: Joachim Landstrom, joachim.landstrom@fek.uu.se

"""

__updated__ = "2021-12-06 10:00:00"

# IMPORT PACKAGES
import copy as cp
import csv
import pathlib as pl
import datetime as dt  # For parsing timestamps incl timezone
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


# DEFINITIONS
def read_csv_file(in_file):
    """Read in_file as csv file with tab delimiter. Space is treated as NaN.

    Assumes all variables are string variables
    """
    df = pd.read_csv(
        in_file, delimiter="\t", dtype=str, na_values=" ", low_memory=False
    )
    return df


def clean_data(in_dta, in_idx, in_vars):
    """Reads a data frame. Drops missing observations."""

    # Drop if missing second column
    df = in_dta.dropna(subset=[in_idx[1]])
    # Drop if missing all announcement dates
    df = df.dropna(how="all", subset=in_vars)
    # Drop of there exists duplicates of the identification pair
    # 'OrganizationID' and 'PeriodEndDate'
    df = df.sort_values(by=in_idx, na_position="last")
    df = df.drop_duplicates(subset=in_idx, keep="first")
    return df


def create_out_file(out_file, out_header):
    """Input out_file. Deletes file if it exists the saves new with header"""
    if pl.Path.exists(out_file):
        pl.Path.unlink(out_file)
    # Header to output file?
    with open(out_file, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=out_header)
        writer.writeheader()
    return


def save_to_csv_file(in_dta, out_file):
    """Saves the data frame in_dta as out_file.

    out_file is tab separated and saving use 'append' and enconding is
    'utf-8'.
    """

    in_dta.to_csv(
        out_file,
        mode="a",
        sep="\t",
        encoding="utf-8",
        index=False,
        header=False,
    )
    # print('Saved ' + str(len(in_dta)) + ' lines to file ' + str(out_file))
    return


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


# PATHS
# Project Directory
proj_path = pl.Path.home().joinpath("Documents", "research", "refinitiv")

# Path to where I store raw data pertinent to the project
raw_path = proj_path.joinpath("raw")

# Path to my Box Account data/ folder
box_data_path = pl.Path.home().joinpath("box", "data")

# Path to where I output data
out_path = proj_path.joinpath("out")

if __name__ == "__main__":
    """
    This script identifies the association between Eikon's 'OrganizationID' and
    Compustat's equivalent variable 'gvkey'.
    """
    # Load manually collected relations between OrganizationID and gvkey
    gvkey_orgid_manual = read_csv_file(
        pl.Path.joinpath(raw_path, "gvkey_orgid_manual.csv")
    )
    gvkey_orgid_manual = gvkey_orgid_manual[["organizationid", "gvkey"]]

    # Read Eikon's data
    print("Reading the Eikon files")
    eikon_files = ["ISIN", "SEDOL", "CUSIP"]
    # eikon_files = [ISIN', 'SEDOL', 'CUSIP', 'RIC']
    dict_of_df = {}
    for file in eikon_files:
        key_name = file.lower()
        name = key_name + "_orgid.csv"
        dict_of_df[key_name] = read_csv_file(pl.Path.joinpath(raw_path, name))
        dict_of_df[key_name].columns = dict_of_df[key_name].columns.str.lower()
        # print(str(key_name) + ' ' + str(len(dict_of_df[key_name])))

    isin = dict_of_df["isin"]
    isin = isin.drop_duplicates()

    sedol = dict_of_df["sedol"]
    sedol = sedol.drop_duplicates()

    cusip = dict_of_df["cusip"]
    cusip = cusip.drop_duplicates()

    organizationid_all = isin["organizationid"]
    organizationid_all = organizationid_all.append(
        [sedol["organizationid"], cusip["organizationid"]]
    )
    organizationid_all = organizationid_all.drop_duplicates()

    # ric = dict_of_df['ric']
    # ric = ric.drop_duplicates()

    # Read Compustat data
    print("Reading the Compustat files")
    stata_files = ["isin", "sedol", "cusip"]
    dict_of_df = {}
    for file in stata_files:
        key_name = file
        name = "gvkey_iid_" + key_name + "_date_range.dta"
        dict_of_df[key_name] = pd.read_stata(
            pl.Path.joinpath(box_data_path, name), preserve_dtypes=True
        )
        # print(str(key_name) + ' ' + str(len(dict_of_df[key_name])))
        my_header = list(dict_of_df[key_name].columns.values)
        # print(my_header)
        # print(dict_of_df[key_name].dtypes)
    # Load Compustat's name file too
    gvkey_names = pd.read_stata(
        pl.Path.joinpath(box_data_path, "g_names.dta"), preserve_dtypes=True
    )
    gvkey_swe_stata = pd.read_stata(
        pl.Path.joinpath(raw_path, "gvkey_swe.dta"), preserve_dtypes=True
    )

    # Prepare the Compustat dataframes for merger
    isin_stata = dict_of_df["isin"]
    isin_stata = isin_stata[["gvkey", "isin"]]
    tmp = gvkey_names[["gvkey", "isin"]]
    isin_stata = isin_stata.append([tmp])
    isin_stata = isin_stata.dropna(how="any", subset=["gvkey", "isin"])
    isin_stata = isin_stata.drop_duplicates()

    sedol_stata = dict_of_df["sedol"]
    sedol_stata = sedol_stata[["gvkey", "sedol"]]
    tmp = gvkey_names[["gvkey", "sedol"]]
    sedol_stata = sedol_stata.append([tmp])
    sedol_stata = sedol_stata.dropna(how="any", subset=["gvkey", "sedol"])
    sedol_stata = sedol_stata.drop_duplicates()

    cusip_stata = dict_of_df["cusip"]
    cusip_stata = cusip_stata[["gvkey", "cusip"]]
    tmp = gvkey_names[["gvkey", "cusip"]]
    cusip_stata = cusip_stata.append([tmp])
    cusip_stata = cusip_stata.dropna(how="any", subset=["gvkey", "cusip"])
    cusip_stata = cusip_stata.drop_duplicates()

    gvkey_all = isin_stata[["gvkey"]]
    tmp = sedol_stata[["gvkey"]]
    gvkey_all = gvkey_all.append([tmp])
    tmp = cusip_stata[["gvkey"]]
    gvkey_all = gvkey_all.append([tmp])
    gvkey_all = gvkey_all.drop_duplicates()  # All gvkey in Compustat

    # Merge Eikon and Compustat, keep the pairs 'gvkey' and 'ric'
    print("Merging Eikon and Compustat")
    isin_df = pd.merge(isin, isin_stata, how="inner", on=["isin"])
    isin_df = isin_df[["organizationid", "gvkey"]]
    isin_df = isin_df.drop_duplicates()

    sedol_df = pd.merge(sedol, sedol_stata, how="inner", on=["sedol"])
    sedol_df = sedol_df[["organizationid", "gvkey"]]
    sedol_df = sedol_df.drop_duplicates()

    cusip_df = pd.merge(cusip, cusip_stata, how="inner", on=["cusip"])
    cusip_df = cusip_df[["organizationid", "gvkey"]]
    cusip_df = cusip_df.drop_duplicates()

    # Append files to a long df with OrganizationID & gvkey
    # Sort, drop NaN and duplicates
    organizationid_gvkey_df = isin_df.append(
        [sedol_df, cusip_df, gvkey_orgid_manual]
    )
    organizationid_gvkey_df = organizationid_gvkey_df.sort_values(
        by=["organizationid", "gvkey"]
    )
    organizationid_gvkey_df = organizationid_gvkey_df.dropna(
        how="any", subset=["organizationid", "gvkey"]
    )
    organizationid_gvkey_df = organizationid_gvkey_df.drop_duplicates()
    # my_header = list(organizationid_gvkey_df.columns.values)
    # print(my_header)

    # With above, main task is done. Below I identify gvkey not found and
    # organizationid not found

    # Identify missing gvkey
    # Subset gvkey_names to be on the 'modern' period (year 2000-)
    gvkey_modern = gvkey_names[gvkey_names["year2"] >= 2000]
    gvkey_modern = gvkey_modern.dropna(
        how="all", subset=["cusip", "sedol", "isin"]
    )
    gvkey_modern = gvkey_modern[["gvkey"]]
    gvkey_modern = gvkey_modern.drop_duplicates()
    gvkey_modern["match"] = "T"

    gvkey_match = organizationid_gvkey_df[["gvkey"]]
    gvkey_match = gvkey_match.drop_duplicates()
    gvkey_match["match"] = "T"

    gvkey_miss = pd.merge(gvkey_modern, gvkey_match, how="left", on=["gvkey"])
    gvkey_miss = gvkey_miss[gvkey_miss.isnull().values.any(axis=1)]
    gvkey_miss = gvkey_miss[["gvkey"]]
    gvkey_miss = gvkey_miss.drop_duplicates()

    # Subset gvkey_miss to Swedish incorporated firms
    gvkey_swe = pd.merge(gvkey_names, gvkey_miss, how="inner", on=["gvkey"])
    gvkey_swe = gvkey_swe[gvkey_swe["fic"] == "SWE"]
    gvkey_swe = gvkey_swe.dropna(how="all", subset=["cusip", "sedol", "isin"])

    print(len(gvkey_miss))
    r_gvkey = len(gvkey_miss) / len(gvkey_modern)
    print(r_gvkey)
    print(len(gvkey_swe))

    # Identify missing OrganizationID
    organizationid_match = organizationid_gvkey_df["organizationid"]
    organizationid_match = organizationid_match.drop_duplicates()

    r_gvkey = len(organizationid_all) - len(organizationid_match)
    # print(r_gvkey)
    # r_gvkey = len(organizationid_match) / len(organizationid_all)
    # print(r_gvkey)

    # print(len(organizationid_match))

    # Save data
    isin_stata = isin_stata[["isin"]]
    isin_stata = isin_stata.drop_duplicates()
    my_header = list(isin_stata.columns.values)
    create_out_file(pl.Path.joinpath(out_path, "isin_stata.csv"), my_header)
    save_to_csv_file(isin_stata, pl.Path.joinpath(out_path, "isin_stata.csv"))

    sedol_stata = sedol_stata[["sedol"]]
    sedol_stata = sedol_stata.drop_duplicates()
    my_header = list(sedol_stata.columns.values)
    create_out_file(pl.Path.joinpath(out_path, "sedol_stata.csv"), my_header)
    save_to_csv_file(sedol_stata, pl.Path.joinpath(out_path, "sedol_stata.csv"))

    cusip_stata = cusip_stata[["cusip"]]
    cusip_stata = cusip_stata.drop_duplicates()
    my_header = list(cusip_stata.columns.values)
    create_out_file(pl.Path.joinpath(out_path, "cusip_stata.csv"), my_header)
    save_to_csv_file(cusip_stata, pl.Path.joinpath(out_path, "cusip_stata.csv"))

    my_header = list(organizationid_gvkey_df.columns.values)
    create_out_file(
        pl.Path.joinpath(out_path, "gvkey_organizationid.csv"), my_header
    )
    save_to_csv_file(
        organizationid_gvkey_df,
        pl.Path.joinpath(out_path, "gvkey_organizationid.csv"),
    )
    organizationid_gvkey_df.to_stata(
        pl.Path.joinpath(out_path, "gvkey_organizationid.dta"),
        write_index=False,
        data_label="Linktable between Refinitiv's OrganizationID & gvkey",
        variable_labels={
            "OrganizationID": "TR.OrganizationID in Refinitiv",
            "gvkey": "Compustat's gvkey",
        },
        version=119,
    )
    save_to_parquet_file(organizationid_gvkey_df, pl.Path.joinpath(out_path, "gvkey_organizationid.parquet.brotli"), compression="brotli")
    print("File " + "--" + str(pl.Path.joinpath(out_path, "gvkey_organizationid.parquet.brotli")) + "--" + " is saved.")
    print("It has " + str(len(organizationid_gvkey_df)) + " rows, and has the following header:")
    print(my_header)

    # File below outputs rows with missing organizationid
    my_header = list(gvkey_swe.columns.values)
    create_out_file(pl.Path.joinpath(out_path, "gvkey_swe.csv"), my_header)
    save_to_csv_file(gvkey_swe, pl.Path.joinpath(out_path, "gvkey_swe.csv"))
    print("Done")
