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

pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)

# DEFINITIONS
def read_csv_file(
    in_file,
    delimiter="\t",
    dtype="str",
    na_values=" ",
    low_memory=False,
    **kwargs,
):
    """Read in_file as csv file with tab delimiter. Space is treated as NaN.

    Assumes as default that all variables are string variables and that the
    file is tab-separated
    """
    df = pd.read_csv(
        in_file,
        delimiter=delimiter,
        dtype=dtype,
        na_values=na_values,
        low_memory=low_memory,
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


def save_to_csv_file(
    in_dta,
    out_file,
    mode="a",
    sep="\t",
    encoding="utf-8",
    index=False,
    header=False,
    **kwargs,
):
    """Saves the data frame in_dta as out_file.

    out_file is as default tab separated and saving use 'append' as default
    with default enconding 'utf-8'.
    """

    in_dta.to_csv(
        out_file,
        mode=mode,
        sep=sep,
        encoding=encoding,
        index=index,
        header=header,
        **kwargs,
    )
    # print('Saved ' + str(len(in_dta)) + ' lines to file ' + str(out_file))
    return


def save_to_parquet_file(df, file, compression="snappy", **kwargs):
    """
    Enter three arguments and save a dataframe as an Apache Parquet database.

    Arguments:

    df: The dataframe to be saved.

    file: The file name of the Apache Parquet database.

    compression: The database's compression. Available compressions are
    ‘snappy’, ‘gzip’, ‘brotli’, or none.

    Return: Returns an Apache Parquet database with a specific compression.

    Notes:
    The function requires either 'pyarrow', or 'fastparquet'.
    """
    # Drop existing file
    if pl.Path.exists(file):
        pl.Path.unlink(file)
    # Save DB
    df.to_parquet(file, compression=compression, **kwargs)


def delisted_ric(in_df):
    """Find RICs delisted, strip delisting suffix, and append to original"""
    delisted = in_df[in_df.ric.str.contains("\^[A-Z][0-9]{2}")]
    delisted_original = delisted["ric"].str.replace(
        r"\^[A-Z][0-9]{2}", "", regex=True, case=True
    )
    delisted_original = delisted_original.to_frame()
    delisted_original = delisted_original.drop_duplicates()
    df = in_df.append(delisted_original)
    df = df.drop_duplicates()
    df = df.dropna(how="any")
    return df


def ipo_ric(in_df):
    """Find IPO RICs and strip the IPO- prefix, and append to original"""
    ipo = in_df[in_df.ric.str.contains("^IPO-")]
    ipo_original = ipo["ric"].str.replace(r"^IPO-", "", regex=True, case=True)
    ipo_original = ipo_original.to_frame()
    ipo_original = ipo_original.drop_duplicates()
    df = in_df.append(ipo_original)
    df = df.drop_duplicates()
    df = df.dropna(how="any")
    return df


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
        pl.Path.joinpath(raw_path, "gvkey_organizationid_manual.csv")
    )
    gvkey_orgid_manual = gvkey_orgid_manual[["organizationid", "gvkey"]]

    # Read Eikon's data and add OrganizationID to ISIN, SEDOL, CUSIP
    print("Reading the Eikon files")
    eikon_files = ["organizationid", "isin", "sedol", "cusip"]
    # eikon_files = [ISIN', 'SEDOL', 'CUSIP', 'RIC']
    dict_of_df = {}
    for file in eikon_files:
        key_name = file.lower()
        name = f"ric_{key_name}.csv"
        dict_of_df[key_name] = read_csv_file(pl.Path.joinpath(raw_path, name))
        dict_of_df[key_name].columns = dict_of_df[key_name].columns.str.lower()
        # print(str(key_name) + ' ' + str(len(dict_of_df[key_name])))

    organizationid = dict_of_df["organizationid"]
    organizationid = ipo_ric(organizationid)
    organizationid = delisted_ric(organizationid)

    isin_eikon = dict_of_df["isin"]
    isin_eikon = ipo_ric(isin_eikon)
    isin_eikon = delisted_ric(isin_eikon)
    isin_eikon = pd.merge(isin_eikon, organizationid, how="inner", on=["ric"])
    isin_eikon = isin_eikon.drop("ric", axis=1)
    isin_eikon = isin_eikon.drop_duplicates()

    sedol_eikon = dict_of_df["sedol"]
    sedol_eikon = ipo_ric(sedol_eikon)
    sedol_eikon = delisted_ric(sedol_eikon)
    sedol_eikon = pd.merge(sedol_eikon, organizationid, how="inner", on=["ric"])
    sedol_eikon = sedol_eikon.drop("ric", axis=1)
    sedol_eikon = sedol_eikon.drop_duplicates()

    cusip_eikon = dict_of_df["cusip"]
    cusip_eikon = ipo_ric(cusip_eikon)
    cusip_eikon = delisted_ric(cusip_eikon)
    cusip_eikon = pd.merge(cusip_eikon, organizationid, how="inner", on=["ric"])
    cusip_eikon = cusip_eikon.drop("ric", axis=1)
    cusip_eikon = cusip_eikon.drop_duplicates()

    organizationid_unique = organizationid["organizationid"]
    organizationid_unique = organizationid_unique.drop_duplicates()

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
    print("Connect Eikon and Compustat firm identification keys")
    isin_df = pd.merge(isin_eikon, isin_stata, how="inner", on=["isin"])
    isin_df = isin_df[["organizationid", "gvkey"]]
    isin_df = isin_df.drop_duplicates()

    sedol_df = pd.merge(sedol_eikon, sedol_stata, how="inner", on=["sedol"])
    sedol_df = sedol_df[["organizationid", "gvkey"]]
    sedol_df = sedol_df.drop_duplicates()

    cusip_df = pd.merge(cusip_eikon, cusip_stata, how="inner", on=["cusip"])
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

    # print(len(gvkey_miss))
    # r_gvkey = len(gvkey_miss) / len(gvkey_modern)
    # print(r_gvkey)
    # print(len(gvkey_swe))
    # print(gvkey_swe)

    # Identify missing OrganizationID
    organizationid_match = organizationid_gvkey_df["organizationid"]
    organizationid_match = organizationid_match.drop_duplicates()

    r_gvkey = len(organizationid_unique) - len(organizationid_match)
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

    organizationid_gvkey_df = organizationid_gvkey_df.rename(
        columns={
            "organizationid": "OrganizationID",
        }
    )
    my_header = list(organizationid_gvkey_df.columns.values)
    create_out_file(
        pl.Path.joinpath(out_path, "organizationid_relations.csv"), my_header
    )
    save_to_csv_file(
        organizationid_gvkey_df,
        pl.Path.joinpath(out_path, "organizationid_relations.csv"),
    )
    organizationid_gvkey_df.to_stata(
        pl.Path.joinpath(out_path, "organizationid_relations.dta"),
        write_index=False,
        data_label="Linktable between Refinitiv's OrganizationID & gvkey",
        variable_labels={
            "OrganizationID": "TR.OrganizationID in Refinitiv",
            "gvkey": "Compustat's gvkey",
        },
        version=119,
    )
    save_to_parquet_file(
        organizationid_gvkey_df,
        pl.Path.joinpath(out_path, "organizationid_relations.parquet.brotli"),
        compression="brotli",
    )
    print(
        "File "
        + "--"
        + str(pl.Path.joinpath(out_path, "organizationid_relations.parquet.brotli"))
        + "--"
        + " is saved."
    )
    print(
        "It has "
        + str(len(organizationid_gvkey_df))
        + " rows, and has the following header:"
    )
    print(my_header)

    # File below outputs rows with missing organizationid
    my_header = list(gvkey_swe.columns.values)
    create_out_file(pl.Path.joinpath(out_path, "gvkey_swe.csv"), my_header)
    save_to_csv_file(gvkey_swe, pl.Path.joinpath(out_path, "gvkey_swe.csv"))
    print("Done")
