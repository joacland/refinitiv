"""
Created on 5 Dec 2021

@author: Joachim Landström, joachim.landstrom@fek.uu.se

This script reads a set of csv files with IBES interim announcement dates
downloaded from Refinitiv, collates them into a single, removes NaN and duplicates,
and replaces the RIC identifier with corresponding OrganizationID.

It also adjusts the time zone for the timestamps to be 'CET'.
Data is saved into an Apache Parquet file with compression set to be 'brotli'.
It also save the data as a Stata-file.

File naming convention for the raw csv files are e.g.
anndats_act_fs_2005.csv, where fs can be either {'fs', 'fq'}. 'fs' is
interim reporting frequency semi-annual, whereas 'fq' is quarterly
frequency. 2005 is the year. The range of years are set with 'first_year'
and 'last_year'.

Any deviation of the naming convention for the raw csv-files renders adj
to the create_source_file_stem function.

The key in the announcement dates files is RIC, but RIC is per QuoteID,
whereas the announcement dates are per OrganizationID. This script
therefore replaces the RIC key with the OrganizationID. The RIC-OrganizationID relation
is not necessarily unique but may be one-to-many OrganizatioIDs.
I use the date range for Refinitiv's PeriodEndDate to match the period end date
to the correct OrganizationID (when necessary).

There may be more than one announcement info per RIC-PeriodEndDate, and, if so,
I only retain that which has most announcement dates (up to five possible announcements).

Input required:
A set of raw-data file names as e.g. anndats_act_fs_2005.csv placed in the 'raw' directory
relations_file_name: Name of file with OrganizationID and RICs
fundamentals_date_range: Name of file with the date range for period end dates
"""

# IMPORT PACKAGES
import pathlib as pl
import pandas as pd
from datetime import datetime, timedelta
from src.my_functions import own_functions as own

# SET PANDAS CONFIGURATION
pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)


def create_source_file_stem(prepend, rep_freq, year):
    """
    Enter three arguments and get the stem name of the source file.

    Arguments:
    prepend -- First string of the file name (e.g. anndats_act).

    rep_freq -- Middle string denoting the report frequency. E.g. FQ or FS.

    year -- The year for the data. E.g. YYYY as in 2021.

    Return:
    A string with the source file's stem
    """

    fid = (
        prepend + "_" + str(rep_freq.lower()) + "_" + str(year) + ".csv"
    )  # Name of source file
    return fid


if __name__ == "__main__":
    # Year range
    first_year = 2000
    last_year = 2022

    # Project directories
    proj_path = pl.Path.home().joinpath("Documents", "research", "refinitiv")
    box_path = pl.Path.home().joinpath("box", "data")

    # Name of source and output directory (inside project directory from above)
    source_dir = proj_path.joinpath("raw")
    out_dir = proj_path.joinpath("out")

    # Raw files name prefix
    file_prefix = "anndats_act"

    # Out file names of data
    out_file_name = f"{file_prefix}_raw.parquet.snappy"  # Name of raw out file
    out_path = pl.Path.joinpath(out_dir, out_file_name)  # Out path for raw out
    out_file_name_final = (
        f"{file_prefix}_organizationid.parquet.brotli"  # Name of final out file
    )
    out_path_final = pl.Path.joinpath(
        out_dir, out_file_name_final
    )  # Out path for final data

    # Additional files necessary
    relations_file_name = pl.Path.joinpath(
        box_path, "refinitiv_relations.parquet.brotli"
    )
    fundamentals_date_range = pl.Path.joinpath(
        box_path, "refinitiv_fundamentals_date_range.csv"
    )

    # Interim frequency
    in_freq = ["fs", "fq"]

    # PROCESS START
    print("Collating announcement dates into a single file")
    for my_year in range(first_year, last_year + 1):
        print("   - Year: " + str(my_year))
        for report_type in in_freq:
            # file name convention anndats_act_fs_2005.csv
            source_file_name = create_source_file_stem(
                file_prefix, report_type, my_year
            )
            source_path = own.create_source_path(source_dir, source_file_name)
            dta = own.read_csv_file(source_path)
            # Parse PeriodEndDate into datetime object
            dta["PeriodEndDate"] = pd.to_datetime(dta["PeriodEndDate"], utc=False)
            if my_year == first_year and report_type == "fs":
                my_header = list(dta.columns.values)
                my_idx = list(my_header[0:2])
                my_vars = list(my_header[2:7])
            for ts in my_vars:
                # Parse timestamp into datetime object, and standardize tz to UTC
                dta[ts] = pd.to_datetime(dta[ts], utc=True)
                # Convert timestamp to tz 'CET'
                dta[ts] = dta[ts].dt.tz_convert("CET")
            dta = dta.sort_values(by=my_idx)
            dta = own.clean_data(dta, my_idx, my_vars)
            # save_to_csv_file(dta, out_path)
            if my_year == first_year and report_type == "fs":
                # Delete any existing file and save new
                own.save_to_parquet_file(dta, out_path, compression="snappy")
            else:
                # Append data to file
                old_dta = pd.read_parquet(out_path)
                dta = pd.concat([old_dta, dta])
                own.save_to_parquet_file(dta, out_path, compression="snappy")

    print("Finalizing raw dataset")
    dta = pd.read_parquet(out_path)
    # Make PeriodEndDate into datetime object
    my_header = list(dta.columns.values)
    my_idx = list(my_header[0:2])
    my_vars = list(my_header[2:7])
    # dta[my_idx[1]] = pd.to_datetime(dta[my_idx[1]], utc=False)  # Already datetime object
    dta = dta.sort_values(by=my_idx)
    dta = own.clean_data(dta, my_idx, my_vars)
    own.save_to_parquet_file(dta, out_path, compression="snappy")

    # Read in data
    print("Post-processing the raw dataset into a final dataset")
    ## Read the announcement dates file
    dta = pd.read_parquet(out_path)
    dta["ric"].replace(
        {r"\^[A-Z][0-9]{2}": ""}, inplace=True, regex=True
    )  # Find RIC-series delisted and the strip delisting suffix
    dta["ric"].replace({r"^IPO-": ""}, inplace=True, regex=True)  # Drop any IPO prefix

    ### Keep only a single row per ric-PeriodEndDate, keep row with most data, or, if same, based on least diff from below.
    dta["non_na"] = dta.iloc[:, 2:].count(
        1
    )  # Nos of non announcement dates (of 5 possible date columns)
    dta["diff"] = (
        pd.to_datetime(dta["OriginalAnnouncementDate"], utc=False).dt.date
        - pd.to_datetime(dta["EPSActReportDate"], utc=False).dt.date
    )
    dta["diff2"] = (
        pd.to_datetime(dta["OriginalAnnouncementDate"], utc=False).dt.date
        - pd.to_datetime(dta["EPSFRActReportDate"], utc=False).dt.date
    )
    dta["diff3"] = (
        pd.to_datetime(dta["EPSFRActReportDate"], utc=False).dt.date
        - pd.to_datetime(dta["EBITActReportDate"], utc=False).dt.date
    )
    dta.sort_values(
        by=["ric", "PeriodEndDate", "non_na", "diff", "diff2", "diff3"],
        inplace=True,
        ascending=[True, True, False, True, True, True],
        na_position="last",
    )
    dta.drop_duplicates(subset=["ric", "PeriodEndDate"], keep="first", inplace=True)
    dta.drop(columns=["non_na", "diff", "diff2", "diff3"], inplace=True)
    dta.rename(columns={"ric": "RIC"}, inplace=True)
    ## Read the OrganizationID-RIC relations file
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
        days=93
    )  # Set firstdt (first available PeriodEndDate) 90(+3, for safety) days back

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
    dta = dta.drop_duplicates(keep="first")
    # Sort

    dta["non_na"] = dta.iloc[:, 2:].count(
        1
    )  # Nos of non announcement dates (of 5 possible date columns)
    dta.sort_values(
        by=["OrganizationID", "PeriodEndDate", "non_na"],
        inplace=True,
        ascending=[True, True, False],
        na_position="last",
    )
    dta.drop_duplicates(
        subset=["OrganizationID", "PeriodEndDate"], keep="first", inplace=True
    )  # Keep the obs with most 'non_na' when duplicate exist
    dta.drop(columns=["non_na"], inplace=True)
    # Set outfile names and save
    my_header = list(dta.columns.values)
    own.save_to_parquet_file(dta, out_path_final, compression="brotli")
    out_path = proj_path.joinpath("out", f"{file_prefix}_organizationid.dta")
    # Strip timezone from timestamps. Timestamps are already set to CET.
    # Stata can't interpret Python timestamps with time zone info.
    my_vars = list(my_header[2:7])
    for ts in my_vars:
        dta[ts] = pd.to_datetime(dta[ts], utc=False).dt.date  # Strips time from timestamp but removes datetime object
        dta[ts] = pd.to_datetime(dta[ts], utc=False)  # Reintroduces the datetime object
    dta.to_stata(
        out_path,
        write_index=False,
        data_label="Interim Report Announcement Dates from Refinitiv Eikon",
        variable_labels={
            "OrganizationID": "TR.OrganizationID in Refinitiv",
            "PeriodEndDate": "TR.F.PeriodEndDate (Fiscal Period End Date)",
            "OriginalAnnouncementDate": "TR.OriginalAnnouncementDate",
            "EPSActReportDate": "TR.EPSActReportDate (IBES report date for EPS [main var])",
            "EPSFRActReportDate": "TR.EPSFRActReportDate (IBES report date for EPS Reported [not main var])",
            "EBITActReportDate""EBITActReportDate": "TR.EBITActReportDate. IBES report date for EBIT [not main var]",
            "EBITDAActReportDate": "TR.EBITDAActReportDate.IBES report date for EBITDA [not main var]",
        },
        convert_dates={"PeriodEndDate": "td"},
        version=119,
    )

    print("File " + "--" + str(out_path_final) + " --" + " is saved.")
    print("It has " + str(len(dta)) + " rows, and has the following header:")
    print(my_header)
    print("Done")
