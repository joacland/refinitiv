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
from src.my_functions import own_functions as own

pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)


if __name__ == "__main__":
    # PROJECT DIRECTORY
    proj_path = pl.Path.home().joinpath("Documents", "research", "refinitiv")
    box_path = pl.Path.home().joinpath("Box", "Data")

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
    # or 9 (cannot find external evidence [in press release, in invitation to
    # annual shareholders' meeting, or, sometimes, in either press release with
    # financial calendar or in the Q4 report]).
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

    orgid_ric = own.read_csv_file(orgid_ric_path)

    # Find IPO RICs and strip the IPO- prefix, and append to original
    ipo = orgid_ric[orgid_ric.RIC.str.contains("^IPO-")]
    ipo_original = ipo["RIC"].str.replace(r"^IPO-", "", regex=True, case=True)
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
        convert_dates={"datadate": "td", "anndats_act": "td"},
        variable_labels={
            "OrganizationID": "TR.OrganizationID in Refinitiv",
            "anndats_act": "Announcement Date",
            "datadate": "Fiscal Period End Date",
        },
        version=119,
    )
