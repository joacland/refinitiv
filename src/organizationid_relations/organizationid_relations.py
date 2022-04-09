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
from src.my_functions import own_functions as own

pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)

# DEFINITIONS
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
# box_data_path = pl.Path.home().joinpath("box", "data")

# Path to where I output data
out_path = proj_path.joinpath("out")

if __name__ == "__main__":
    """
    This script collates all relations between OrganizationID and InstrumentID, QuoteID, RIC, ISIN, and SEDOL that I can find.
    """
    # print("Reading the Eikon files")
    eikon_files = [
        "ric",
        "isin",
        "sedol",
        "quoteid",
        "instrumentid",
        "organizationid",
    ]  # Missing cusip
    ids = [
        "OrganizationID",
        "UltimateParentID",
        "InstrumentID",
        "QuoteID"
    ]
    dict_of_df = {}
    for file in eikon_files:
        key_name = file.lower()
        name = f"instrument_data_{key_name}_v2.csv"
        dict_of_df[key_name] = own.read_csv_file(
            pl.Path.joinpath(raw_path, name),
            parse_dates=["FirstTradeDate", "RetireDate"]
        )
        # dict_of_df[key_name].columns = dict_of_df[key_name].columns.str.lower()
        # print(str(key_name) + " " + str(len(dict_of_df[key_name])))
        # print(dict_of_df[key_name].columns.values)

    ric_eikon = dict_of_df["ric"]
    # print(ric_eikon.head())
    isin_eikon = dict_of_df["isin"]
    sedol_eikon = dict_of_df["sedol"]
    # cusip_eikon = dict_of_df["cusip"]
    quote_eikon = dict_of_df["quoteid"]
    instrumentid_eikon = dict_of_df["instrumentid"]
    organizationid_eikon = dict_of_df["organizationid"]
    frames = [ric_eikon, isin_eikon, sedol_eikon, quote_eikon, instrumentid_eikon, organizationid_eikon]
    dta = pd.concat(frames)
    dta = dta.drop("Instrument", axis=1)
    dta = dta.dropna(subset=["OrganizationID"])
    for id in ids:
        dta[id] = dta[id].str.replace(r"\.0", "", regex=True)  # Make sure vars don't appear as floats (with decimals)
    my_header = list(dta.columns.values)
    print(my_header)
    dta = dta.sort_values(my_header)
    dta = dta.drop_duplicates(subset=my_header[1:])

    # sbb = dta[dta.OrganizationID == "5044034256"]
    # quoteid_ric = dta[["InstrumentID", "FirstTradeDate", "RetireDate"]]
    # quoteid_ric = quoteid_ric.dropna(how="any", subset=["InstrumentID"])
    # quoteid_ric["FirstTradeDate"] = quoteid_ric["FirstTradeDate"].fillna("1999-12-31")
    # quoteid_ric["FirstTradeDate"] = quoteid_ric.groupby("InstrumentID").FirstTradeDate.transform("min")
    # quoteid_ric["RetireDate"] = quoteid_ric["RetireDate"].fillna("2021-12-31")
    # quoteid_ric["RetireDate"] = quoteid_ric.groupby("InstrumentID").RetireDate.transform("max")
    # quoteid_ric = quoteid_ric[quoteid_ric.FirstTradeDate < quoteid_ric.RetireDate]
    # quoteid_ric["RetireDate"] = quoteid_ric["RetireDate"].mask(quoteid_ric["RetireDate"] > "2021-12-31", "2021-12-31")
    # quoteid_ric["FirstTradeDate"] = quoteid_ric["FirstTradeDate"].mask(quoteid_ric["FirstTradeDate"] < "1999-12-31", "1999-12-31")
    # quoteid_ric.rename({"FirstTradeDate": "SDate", "RetireDate": "EDate"}, axis=1, inplace=True)
    # quoteid_ric = quoteid_ric.sort_values("InstrumentID")
    # quoteid_ric.drop_duplicates(inplace=True)
    # quoteid_ric["SDate"] = quoteid_ric["SDate"].dt.strftime("%Y-%m-%d")
    # quoteid_ric["EDate"] = quoteid_ric["EDate"].dt.strftime("%Y-%m-%d")
    # quoteid_ric.set_index("InstrumentID")
    # test_dict = quoteid_ric.to_dict("index")
    # print(test_dict)

    # print(sbb)
    # print(dta[dta.RIC == "EFFN.ST"])
    # print(dta[dta.InstrumentID == "15629715433"])
    # print(quoteid_ric.info(verbose=True))

    dta.to_stata(
        pl.Path.joinpath(out_path, "refinitiv_relations.dta"),
        write_index=False,
        data_label="Linktable between Refinitiv's various ID variables",
        variable_labels={
            "OrganizationID": "TR.OrganizationID in Refinitiv",
            "UltimateParentID": "TR.UltimateParentID",
            "InstrumentID": "TR.InstrumentID",
            "QuoteID": "TR.QuoteID"
        },
        version=119,
    )
    save_to_parquet_file(
        dta,
        pl.Path.joinpath(out_path, "refinitiv_relations.parquet.brotli"),
        compression="brotli",
    )
    save_to_csv_file(dta, pl.Path.joinpath(out_path, "refinitiv_relations.csv"), mode="w", header=True)
    print("Done")
