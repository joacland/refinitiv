"""
Created on 26 nov. 2021
@author: joachim landström

Based on a list of OrganizationIDs

Retrieves a single Eikon instrument variable and saves it into a single csv-file.
The script generates a -raw- file.

Frequency is Interim

The output file is prefixed with the variable name in lower case
(and suffixed csv). The csv-file is tab separated.

Input required
---------------
Source file name: Name of file with OrganizationID
Source file path: Where is the list
Out file path: Where to put the output file?
FIRST_YEAR: First fiscal year of data.
"""

import csv
import os
# import pathlib as pl
import sys
import time  # For sleep functionality
from datetime import datetime
from src.my_functions import own_functions as own

import eikon as ek  # the Eikon Python wrapper package
import pandas as pd


# import pyarrow as pa
# import pyarrow.parquet as pq


def create_source_file_name(in_prefix, in_type, in_yr):
    fid = (
            in_prefix + "_" + str(in_type.lower()) + "_" + str(in_yr) + ".csv"
    )  # Name of source file
    return fid


def create_source_path(in_file):
    path = proj_path.joinpath("raw", source_file_name)
    return path


def create_out_file(out_file, out_header):
    """ Input out_file. Deletes file if it exists the saves new with header
    """
    if pl.Path.exists(out_file):
        pl.Path.unlink(out_file)
    # Header to output file?
    with open(out_file, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=out_header)
        writer.writeheader()
    return


def save_to_csv_file(in_dta, out_file):
    """ Saves the data frame in_dta as out_file.

    out_file is tab separated and saving use 'append' and encoding is
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


def save_to_parquet_file(in_dta, out_file, compression):
    """ Saves the data frame 'in_dta' as an Apache Parquet db named 'out_file'
    and it has compression 'compression'.

    The 'out_file' is an Apache Parquets database.

    Requires either 'pyarrow', or 'fastparquet'.

    compression: {‘snappy’, ‘gzip’, ‘brotli’, None}
    """
    # Drop existing file
    if pl.Path.exists(out_file):
        pl.Path.unlink(out_file)
    # Save DB
    in_dta.to_parquet(out_file, compression=compression)
    return


def read_csv_file(in_file):
    """ Read in_file as csv file with tab delimiter. Space is NaN."""
    df = pd.read_csv(in_file, delimiter="\t", na_values=" ", low_memory=False)
    return df


def clean_data(in_dta, in_idx, in_vars):
    """ Reads an anndats data frame. Drops missing observations."""

    # Drop if missing PeriodEndDate
    df = in_dta.dropna(subset=[in_idx[1]])
    # Drop if missing all announcement dates
    df = df.dropna(how="all", subset=in_vars)
    # Drop of there exists duplicates of the identification pair
    # 'ric' and 'PeriodEndDate'
    df = df.drop_duplicates(subset=in_idx, keep="first")
    return df


if __name__ == "__main__":
    # SET THE EIKON CONFIGURATION
    ek.set_timeout(300)  # Set Eikon's timeout to be 5 min.
    # insert APP_KEY from app key generator in eikon
    ek.set_app_key("1418cf51ee9046a3a767d6f8c871c1d3fcaf1953")
    SIZE = 7000  # Number of rows gathered per Eikon-loop
    print(sys.version)
    print(ek.__version__)

    # WHICH VARIABLE(S) TO RETRIEVE?
    # OWN_VAR = 'PeriodEndDate'
    # OWN_TR_VAR = 'TR.F.' + str(OWN_VAR)
    header = [
        "OrganizationID",
        "PeriodEndDate",
    ]

    # INTERIM REPORT FREQUENCY
    # Tried eg. 1CQ2013 but CQ won't give IBES Report Dates. Must be FQ/FS
    # REP_FREQ = ['FS', 'FQ']
    # REP_FREQ = ["FQ", "FS"]
    REP_FREQ = ["FQ"]

    # FROM WHICH VARIABLE?
    # START_DATE = '2021-09-30'  # Start date. Set it to be equal to last calendar qtr
    START_MONTH = "12"  # For rolling Sdates
    START_DAY = "31"  # For rolling SDates
    FIRST_YEAR = 2022
    # Adding +1 to hedge any possible missmatch btw fyr & cal-yr
    LAST_YEAR = datetime.now().year + 1
    MIN_QTR = 1

    # WHERE IS, AND WHERE TO PUT, DATA?
    SOURCE_PATH = "G:\\"  # where is?
    # SOURCE_PATH = 'C:\\Users\\joach\\OneDrive\\Dokument'  # where is?
    OUT_PATH = "G:\\"  # where to?

    # FILE NAMES OF DATA
    # SOURCE_FNAME = 'rics_cleaned_v2'  # Name of source file
    SOURCE_FNAME = "organizationid_cleaned"  # Name of source file
    SOURCE_FNAME_SUFFIX = ".csv"  # Source file type

    OUT_FNAME = "anndats_act"  # Name of output file
    OUT_FNAME_SUFFIX = ".csv"  # Output file type

    if __name__ == "__main__":

        # PREPARE FILES
        # File name concatenation
        S_FNAME = SOURCE_FNAME + SOURCE_FNAME_SUFFIX
        SOURCE_FNAME_CPL = os.path.join(SOURCE_PATH, S_FNAME)

        # READ THE DATA FROM SOURCE FILE
        # File has header. make it into a list
        own_list = pd.read_csv(
            SOURCE_FNAME_CPL, dtype={"OrganizationID": str}, low_memory=False
        )
        own_list = own_list["OrganizationID"].values.tolist()
        print(
            "No of OrganizationIDs to retrieve data for: "
            + str(len(own_list))
        )

        # RETRIEVE DATA FROM EIKON
        for midix in REP_FREQ:
            # IF bi-annuals, there's only two reports per year, otherwise there's four.
            if midix == "FS":
                MAX_QTR = 2
            else:
                MAX_QTR = 4

            for yr in range(FIRST_YEAR, LAST_YEAR + 1):
                # OUT FILE MGMT
                # File per year
                O_FNAME = (
                        OUT_FNAME
                        + "_"
                        + str(midix.lower())
                        + "_"
                        + str(yr)
                        + OUT_FNAME_SUFFIX
                )
                OUT_FNAME_CPL = os.path.join(OUT_PATH, O_FNAME)

                # Remove output file, if it exist
                if os.path.exists(OUT_FNAME_CPL):
                    os.remove(OUT_FNAME_CPL)

                # Header to output file?
                with open(
                        OUT_FNAME_CPL, "w", encoding="UTF8", newline=""
                ) as f:
                    writer = csv.DictWriter(
                        f, delimiter="\t", fieldnames=header
                    )
                    writer.writeheader()

                for qtr in range(MIN_QTR, MAX_QTR + 1):
                    # What period? E.g. 1FS2020, or 1FQ2020
                    period = f"{qtr}{midix}{yr}"
                    # If rolling start date (SDate)? What should it be?
                    # s_dte = START_DATE
                    s_yr = yr + 1
                    s_dte = (
                            str(s_yr)
                            + "-"
                            + str(START_MONTH)
                            + "-"
                            + str(START_DAY)
                    )
                    print(" - Period: " + str(period))
                    own_fields = [
                        ek.TR_Field(
                            "TR.TotalAssetsReported.periodenddate",
                            {
                                "SDate": s_dte,
                                "Period": period,
                                "RollPeriods": "False",
                                # 'Scale': '6',s
                                "AlignType": "PeriodEndDate",
                                # 'ReportingState': 'Orig',
                                # 'ReportType': 'Final'
                            }
                        )

                    ]
                    # The actual retrieval loop
                    # I run this in sections to avoid other types of errors such as 'timeout'
                    # errors
                    for line_start in range(0, len(own_list) + 1, SIZE):
                        line_end = line_start + SIZE
                        print(
                            "   + Lines: "
                            + str(line_start)
                            + "/"
                            + str(line_end)
                        )
                        # Have added a retry loop if error since sometimes there are
                        # problems in the API-connection
                        for rec_attempts in range(10):
                            try:
                                dta, err = ek.get_data(
                                    instruments=own_list[line_start:line_end],
                                    fields=own_fields,
                                    field_name=False,
                                    raw_output=False,
                                )
                            except Exception as own_err:
                                print(
                                    "Exception in attempt #"
                                    + str(rec_attempts)
                                    + ": "
                                    + str(own_err)
                                    + ", was raised. Trying again."
                                )
                                # Try again after a sleep of 30 seconds
                                time.sleep(30)
                                continue
                            else:
                                break
                        else:
                            # All attempts failed
                            print("All attempts have failed: Program aborted")
                            sys.exit()

                        # Saves the retrieved Eikon data to out-file
                        if dta is not None:
                            if not dta.empty:
                                # Drop empty rows
                                my_header = list(dta.columns.values)
                                dta = dta.dropna(how="any", subset=my_header[1:])
                                # Remove any duplicates
                                dta = dta.drop_duplicates()
                            # Appends the retrieved Eikon data to out-file, unless empty dta
                            if not dta.empty:
                                dta["date"] = pd.to_datetime(dta["date"])
                                dta["firstdt"] = dta["date"].min()
                                dta["lastdt"] = dta["date"].max()
                                dta["firstdt"] = dta["firstdt"].dt.strftime("%Y-%m-%d")
                                dta["lastdt"] = dta["lastdt"].dt.strftime("%Y-%m-%d")
                                dta = dta[["InstrumentID", "firstdt", "lastdt"]]
                                dta = dta.drop_duplicates()
                                # print(f"     From {dta['firstdtf']} to {dta['lastdt']}")
                                # Only save data to file once per loop
                                if pl.Path.exists(out_file):
                                    own.save_to_csv_file(dta, out_file)
                                else:
                                    own.save_to_csv_file(dta, out_file, header=True, mode="w")
                            else:
                                no_data["InstrumentID"] = [qte]
                                if pl.Path.exists(err_file):
                                    own.save_to_csv_file(no_data, err_file)
                                else:
                                    own.save_to_csv_file(no_data, err_file, header=True, mode="w")
                                print(f"     No data")

                        dta.to_csv(
                            OUT_FNAME_CPL,
                            mode="a",
                            sep="\t",
                            encoding="utf-8",
                            index=False,
                            header=False,
                        )
                        # Pause for 10s to reduce risk of throwing an exception
                        time.sleep(10)
        print("DONE")
