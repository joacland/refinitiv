"""
Created on 5 Dec 2021

@author: Joachim Landstrom, joachim.landstrom@fek.uu.se

This script reads an ods file with data collected from Refinitiv's
Advanced Filing Search with 'Filing Dates' for fiscal reports (annual or interim).
The filing dates mostly corresponds to the actual announcement dates and the file
has been corrected for possible date errors. For all practical purposes the file
holds fiscal report announcement dates.

The script replaces the RIC key with corresponding OrganizationID.
RIC is per QuoteID, whereas the announcement dates are per OrganizationID.
This script therefore replaces the RIC key with the OrganizationID.
The RIC-OrganizationID relation is not necessarily unique but may be
one-to-many OrganizationIDs. I use the date range for Refinitiv's PeriodEndDate
to match the period end date to the correct OrganizationID (when necessary).

The script also generates an error file that identifies any RIC the does not
exist in the relations file but that exists in the announcement file. Typos in the
announcement file RIC e.g. populate this file.

Data is saved into an Apache Parquet file with compression set to be 'brotli'.
It also saves the data as a Stata-file.

Input required:
raw_file: The ods-file with the main data. Should be in the raw-directory and called e.g.
            anndats_ar_eikon_stripped.ods or,
            anndats_qr_eikon_stripped.ods
frequency: 'interim' or 'annual' (depending on which announcement dates the raw_file holds)
relations_file_name: Name of file with OrganizationID and RICs
fundamentals_date_range: Name of file with the date range for period end dates
"""
import pathlib as pl
import pandas as pd
from pandas_ods_reader import read_ods
from datetime import datetime, timedelta
from src.my_functions import own_functions as own

pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)


if __name__ == "__main__":
    # Project directory
    proj_path = pl.Path.home().joinpath("Documents", "research", "refinitiv")
    box_path = pl.Path.home().joinpath("box", "data")

    # Name of source and output directory (inside project directory from above)
    source_dir = proj_path.joinpath("raw")
    out_dir = proj_path.joinpath("out")

    # Report frequency
    frequency = "interim"  # 'annual', or 'interim'

    # Raw file name
    if frequency == "annual":
        acr = "ar"
    else:
        acr = "qr"
    raw_file = f"anndats_{acr}_eikon_stripped.ods"
    raw_path = pl.Path.joinpath(source_dir, raw_file)  # Path to raw data file

    # Out file name prefix
    file_prefix = f"anndats_{frequency}_refinitiv"

    # File names of data
    out_file_name = f"{file_prefix}.dta"  # Name of STATA out file
    out_path = pl.Path.joinpath(out_dir, out_file_name)  # Out path for output file

    out_file_name_final = f"{file_prefix}.parquet.brotli"  # Name of final out file
    out_path_final = pl.Path.joinpath(
        out_dir, out_file_name_final
    )  # Out path for final data

    # Error file
    err_file_name = f"error_ric_{file_prefix}.csv"  # Name of error file
    err_path = pl.Path.joinpath(out_dir, err_file_name)  # Out path for output file

    # Additional files necessary
    relations_file_name = pl.Path.joinpath(
        box_path, "refinitiv_relations.parquet.brotli"
    )
    fundamentals_date_range = pl.Path.joinpath(
        box_path, "refinitiv_fundamentals_date_range.csv"
    )

    # Date format for the Filing Date and Document Date
    d_format = "%Y-%m-%d"  # E.g. 2020-12-31

    # Read ODS file
    dta = read_ods(raw_path)
    my_header = list(dta.columns.values)
    # Drop if mod == 999 (not publicly traded)
    # or 9 (cannot find external evidence [in press release, in invitation to
    # annual shareholders' meeting, or, sometimes, in either press release with
    # financial calendar or in the Q4 report]).
    dta = dta[dta["mod"] != 999]
    dta = dta[dta["mod"] != 9]
    dta = dta[
        dta["replag"] != 0
    ]  # Report lag is 0, not possible, dropping these (should be checked and corrected)

    dta = dta[
        [
            "Receipt Date",
            "Filing Date",
            "Document Date",
            "RIC",
        ]
    ]
    dta = dta.rename(
        columns={
            "Receipt Date": "ReceiptDate",  # Receipt TimeStamp when recorded by Refinitiv
            "Filing Date": "anndats",  # Announcement Date of the Interim Report
            "Document Date": "PeriodEndDate",  # Fiscal Period End Date
        }
    )
    # Convert datetime strings to Pandas datetime
    dta["ReceiptDate"] = pd.to_datetime(dta["ReceiptDate"])
    for date in ["anndats", "PeriodEndDate"]:
        dta[date] = pd.to_datetime(dta[date], format=d_format)
        # Strip the time from the datetime
        # df[date] = df[date].dt.date

    my_header = list(dta.columns.values)

    dta["RIC"].replace(
        {r"\^[A-Z][0-9]{2}": ""}, inplace=True, regex=True
    )  # Find RIC-series delisted and the strip delisting suffix
    dta["RIC"].replace({r"^IPO-": ""}, inplace=True, regex=True)  # Drop any IPO prefix

    dta.drop_duplicates(
        subset=["RIC", "PeriodEndDate"], keep="first", inplace=True
    )  # Just making sure
    err = dta.copy()  # Copy dta. To be used to build the error file below.

    # Read the OrganizationID-RIC relations file
    relations = pd.read_parquet(relations_file_name)
    relations = relations[["OrganizationID", "RIC"]]
    relations["RIC"].replace(
        {r"\^[A-Z][0-9]{2}": ""}, inplace=True, regex=True
    )  # Find RICs delisted and strip their delisting suffix
    relations["RIC"].replace(
        {r"^IPO-": ""}, inplace=True, regex=True
    )  # Drop any IPO prefix
    relations = relations.dropna(how="any")
    relations = relations.drop_duplicates(keep="first")
    relations["nos"] = relations.groupby("RIC")["RIC"].transform(
        "size"
    )  # Count rows per RIC (+1 -> one RIC to many OrganizationID)
    one_to_one = relations[
        relations.nos == 1
    ]  # Problem-free and can readily be attached an OrganizationID
    one_to_many = relations[
        relations.nos > 1
    ]  # Must make sure that only the OrganizationID that was active during announcement is used
    relations = relations.drop(columns=["nos"])

    ## Subset 'relations' into one-to-one RICs and one-to-many RICS
    one_to_one_ric = one_to_one["RIC"]
    one_to_one_ric = one_to_one_ric.dropna(how="any")
    one_to_one_ric = one_to_one_ric.drop_duplicates(keep="first")
    one_to_many_ric = one_to_many["RIC"]
    one_to_many_ric = one_to_many_ric.dropna(how="any")
    one_to_many_ric = one_to_many_ric.drop_duplicates(keep="first")
    one_to_one_relation = relations.merge(
        one_to_one_ric, how="inner", on="RIC"
    )  # RIC-OrganizationID when RIC-OrganizationID is unique
    one_to_many_relation = relations.merge(
        one_to_many_ric, how="inner", on="RIC"
    )  # RIC-OrganizationID when more than one OrganizationID per RIC

    ## Subset announcement file to those matching the one-to-one relation and add OrganizationID
    one_to_one_dta = one_to_one_relation.merge(dta, how="inner", on="RIC")
    ## Subset announcement file to those matching the one-to-many relation and add OrganizationID
    one_to_many_dta = one_to_many_relation.merge(dta, how="left", on="RIC")
    ### Drop rows where PeriodEndDate is outside the date range for the PeriodEndDate
    date_range = own.read_csv_file(
        fundamentals_date_range, parse_dates=["firstdt", "lastdt"]
    )  # Load data with date range for the PeriodEndDate per OrganizationID
    date_range["firstdt"] = date_range["firstdt"] - timedelta(
        days=7
    )  # Set firstdt (first available PeriodEndDate) a week back (start of fiscal period - 1w) to avoid limit issues

    one_to_many_dta = date_range.merge(
        one_to_many_dta, how="left", on="OrganizationID"
    )  # Add the date range
    one_to_many_dta.dropna(
        how="any", subset=["PeriodEndDate"], inplace=True
    )  # Drop row where PeriodEndDate is NA
    one_to_many_dta.loc[
        (one_to_many_dta["PeriodEndDate"] > one_to_many_dta["lastdt"])
        | (one_to_many_dta["PeriodEndDate"] < one_to_many_dta["firstdt"]),
        "PeriodEndDate",
    ] = 0  # Mark rows where PeriodEndDate is outside the range based on 'firstdt' to 'lastdt'. Sets PeriodEndDate = 0
    one_to_many_dta.drop(
        one_to_many_dta[one_to_many_dta["PeriodEndDate"] == 0].index, inplace=True
    )  # Drop the marked rows
    one_to_many_dta["PeriodEndDate"] = pd.to_datetime(
        one_to_many_dta["PeriodEndDate"], utc=False
    )  # PeriodEndDate has to become datetime again.
    ## Drop help columns and merge data into its original DataFrame
    one_to_one_dta = one_to_one_dta.drop(columns=["RIC"])
    one_to_many_dta = one_to_many_dta.drop(columns=["firstdt", "lastdt", "RIC"])
    dta = pd.concat([one_to_one_dta, one_to_many_dta])
    dta.dropna(
        how="any", subset=["OrganizationID", "PeriodEndDate", "anndats"], inplace=True
    )  # Drop row where some of these cols have NA

    dta.sort_values(
        by=["OrganizationID", "PeriodEndDate", "anndats"],
        inplace=True,
        ascending=[True, True, False],
        na_position="last",
    )
    dta.drop_duplicates(
        subset=["OrganizationID", "PeriodEndDate"], keep="first", inplace=True
    )  # Keep the obs with most 'non_na' when duplicate exist

    # Make error file
    relations.drop_duplicates(subset=["RIC"], inplace=True)
    err.drop_duplicates(subset=["RIC"], inplace=True)
    err = (
        err.merge(relations, how="left", on="RIC", indicator=True)
        .query("_merge == 'left_only'")
        .drop("_merge", axis=1)
    )  # Selects rows from 'err' that does not have a matching key in 'relations'
    err = err["RIC"]
    err.drop_duplicates(inplace=True)
    own.save_to_csv_file(err, err_path, mode="w")  # These RICs exists in 'dta' but not in 'relations'

    # Save files
    my_header = list(dta.columns.values)
    own.save_to_parquet_file(dta, out_path_final, compression="brotli")
    if frequency == "interim":
        label = "Interim"
    else:
        label = "Annual"
    dta.to_stata(
        out_path,
        write_index=False,
        data_label=f"{label} Report Announcement Dates from Refinitiv Eikon",
        convert_dates={"PeriodEndDate": "td", "anndats": "td"},
        variable_labels={
            "OrganizationID": "TR.OrganizationID in Refinitiv",
            "ReceiptDate": "Receipt Timestamp by Refinitiv",
            "anndats": "Announcement Date of Annual Report",
            "PeriodEndDate": "Fiscal Period End Date",
        },
        version=119,
    )
    print("File " + "--" + str(out_path_final) + " --" + " is saved.")
    print("It has " + str(len(dta)) + " rows, and has the following header:")
    print(my_header)
    print("Done")
