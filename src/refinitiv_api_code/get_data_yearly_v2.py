"""
Created on 26 nov. 2021
@author: joachim landström

Based on a list of RICs

Retrieves Eikon variables and saves them to a csv-file.
The script generates two files, a -raw- file and a v2 file. The v2 file has data
where empty rows, and possible duplicate rows, are discarded. Use v2.

Frequency is yearly

Input required
---------------
Source file name: Name of file with RICs with header -ric-
Source file path: Where is the list
Out file path: Where to put the output file?
"""
import csv
import os
import pathlib as pl
import sys
import time  # For sleep functionality
# IMPORT PACKAGES
from datetime import datetime
from src.my_functions import own_functions as own

import eikon as ek  # the Eikon Python wrapper package
import pandas as pd

# SET THE EIKON CONFIGURATION
ek.set_timeout(1000)  # Set Eikon's timeout to be 5 min.
# insert APP_KEY from app key generator in eikon
ek.set_app_key("1418cf51ee9046a3a767d6f8c871c1d3fcaf1953")
SIZE = 1500  # Number of rows gathered per Eikon-loop

# SET PANDAS CONFIGURATION
pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)

# YEAR TYPE
# YEAR_TYPE = "CY"  # CY or FY (Calendar Year or Fiscal Year)

# SDate?
FIRST_YEAR = 1999
# LAST_YEAR = datetime.now().year + 1
LAST_YEAR = 2008

SYM_IN = 'OrganizationID'

# FILE NAMES OF DATA
# SOURCE_FNAME = "ric_v2"  # Name of source file
# SOURCE_FNAME = "organizationid_swe"  # Name of source file
# SOURCE_FILE = pl.Path(r"F:\organizationid_test.csv")
SOURCE_FILE = pl.Path(r"D:\tmp\organizationid_all.csv")
OUT_FILE = pl.Path(r"D:\instrument_data2_organizationid.csv")  # Name of output file
OUT_FILE2 = pl.Path(r"D:\instrument_data2_organizationid_v2.csv")  # Name of output file


if __name__ == "__main__":

    # PREPARE FILES
    # Remove output file, if it exist
    header = [
        # SYM_IN,
        "Instrument",
        "OrganizationID",
        "UltimateParentID",
        "InstrumentID",
        "QuoteID",
        "LegalEntityIdentifier",
        "RIC",
        "ISIN",
        "SEDOL",
        "FirstTradeDate",
        "RetireDate"
    ]

    # Header to output file?
    # with open(OUT_FILE, "w", encoding="UTF8", newline="") as f:
    #     writer = csv.DictWriter(f, delimiter="\t", fieldnames=header)
    #     writer.writeheader()

    # READ THE DATA FROM SOURCE FILE
    # File has header. make it into a list
    own_list = own.read_csv_file(SOURCE_FILE)
    # own_list =pd.read_csv(SOURCE_FNAME_CPL, low_memory=False, dtype=str, sep="\t")
    if SYM_IN == "RIC":
        delisted = own_list[own_list.RIC.str.contains('\^[A-Z][0-9]{2}')]
        delisted_original = delisted["RIC"].str.replace(r'\^[A-Z][0-9]{2}', '', regex=True, case=True)
        delisted_original = delisted_original.to_frame()
        delisted_original = delisted_original.drop_duplicates()
        own_list = own_list.append(delisted_original)
    own_list = own_list.drop_duplicates()
    own_list = own_list[SYM_IN].values.tolist()

    # File has no header. Make it into a list
    # with open(SOURCE_FNAME_CPL) as f:
    #     own_list = [line.rstrip('\n') for line in f]
    # print(len(own_list))

    # RETRIEVE DATA FROM EIKON
    print(f"No of {SYM_IN} to retrieve data for: {str(len(own_list))}. ")
    for yr in range(LAST_YEAR, FIRST_YEAR - 1, -1):
        # What period? E.g. FY2020, or CY2020
        # period = f"{YEAR_TYPE}{yr}"
        # If rolling start date (SDate)? What should it be?
        # sdate = str(yr) + "-" + str(START_MONTH) + "-" + str(START_DAY)
        sdate = f"{yr+1}-12-31"
        # print(" - Period: " + str(period) + ", based on SDate " + str(s_dte))
        print(f" - Per SDate {sdate}.")
        own_dict = {
            # "Period": period,
            "SDate": sdate,
            # "Scale": scale,
            # "ReportType": rep_type,
            # "ReportingState": rep_state,
            # "ConsolBasis": consol_basis,
            # "Curn": curn,
            # "AlignType": align,
            # "RollPeriods": roll,
            # "IncludeSpl": special,
        }
        own_fields = [
            ek.TR_Field("TR.OrganizationID"),
            ek.TR_Field("TR.UltimateParentID"),
            ek.TR_Field("TR.InstrumentID"),
            ek.TR_Field("TR.QuoteID"),
            ek.TR_Field("TR.LegalEntityIdentifier"),
            ek.TR_Field("TR.RIC", own_dict),
            ek.TR_Field("TR.ISIN", own_dict),
            ek.TR_Field("TR.SEDOL", own_dict),
            ek.TR_Field("TR.FirstTradeDate"),
            ek.TR_Field("TR.RetireDate"),
        ]
        # The actual retrieval loop
        # I run this in sections to avoid other types of errors such as 'timeout'
        # errors
        dta_all = pd.DataFrame()  # Just so it is defined
        for line_start in range(0, len(own_list) + 1, SIZE):
            line_end = line_start + SIZE
            print(f" + Lines: {str(line_start)}/{str(line_end)} (Year {yr})")
            # Have added a retry loop if error since sometimes there are
            # problems in the API-connection
            for rec_attempts in range(20):
                try:
                    # Retrieval function get_data
                    dta, err = ek.get_data(
                        instruments=own_list[line_start:line_end],
                        fields=own_fields,
                        field_name=False,
                        raw_output=False,
                    )
                except Exception as own_err:
                    print(
                        f"Exception in attempt # {str(rec_attempts)}: {str(own_err)}, was raised. Trying "
                        f"again. "
                    )
                    # Try again after a sleep of 30 seconds
                    time.sleep(30)
                    continue
                else:
                    break
            else:
                # All attempts failed
                print(f"All attempts have failed: Program aborted while running year {yr}")
                sys.exit()

            if dta is not None:
                if not dta.empty:
                    # Drop empty rows
                    my_header = list(dta.columns.values)
                    dta = dta.dropna(how="all", subset=my_header[1:])

                    # Remove any duplicates
                    dta = dta.drop_duplicates()
                # Appends the retrieved Eikon data to out-file, unless empty dta
                if not dta.empty:
                    # dta = dta.rename(
                    #     columns={"Instruments": "SEDOL"}
                    # )
                    # dta = dta.rename(
                    #     columns={"Instrument": "SEDOL"}
                    # )

                    frames = [dta_all, dta]
                    dta_all = pd.concat(frames)
                    dta_all = dta_all.drop_duplicates()
                    print(f"     dta_all len is {len(dta_all)}")
            time.sleep(1)
        # Only save data to file once per yr
        own.save_to_csv_file(dta_all, OUT_FILE)

    # FIX OUTPUT FILE
    # Revised file name
    # Remove revised out-file, if it exists
    if os.path.exists(OUT_FILE2):
        os.remove(OUT_FILE2)

    # Write header to revised out file
    with open(OUT_FILE2, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=header)
        writer.writeheader()

    # Read the original out file
    dta = own.read_csv_file(OUT_FILE)
    print(f"Length of the original file is {len(dta)}.")

    # Drop empty rows
    my_header = list(dta.columns.values)
    my_data = list(my_header[2:])
    dta.dropna(how="all", subset=my_data, inplace=True)
    dta.dropna(how="all", subset=my_data, inplace=True)

    # Remove any duplicates
    dta.drop_duplicates(inplace=True)
    print(f"Length of the cleansed file is now {len(dta)}.")

    # Save data
    own.save_to_csv_file(dta, OUT_FILE2)

    print("DONE")
