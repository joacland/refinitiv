"""
Created on 26 nov. 2021
@author: joachim landström

Based on a list of RICs

Retrieves a single Eikon instrument variable and saves it into a single csv-file.
The script generates two files, a -raw- file and a v2 file. The v2 file has data
where empty rows, and possible duplicate rows, are discarded. Use v2.

Frequency is yearly

The output file is prefixed with the variable name in lower case
(and suffixed csv). The csv-file is tab separated.

Input required
---------------
Source file name: Name of file with RICs with header -ric-
Source file path: Where is the list
Out file path: Where to put the output file?
"""
import csv
import os
import sys
import time  # For sleep functionality
# IMPORT PACKAGES
from datetime import datetime

import eikon as ek  # the Eikon Python wrapper package
import pandas as pd

# SET THE EIKON CONFIGURATION
ek.set_timeout(300)  # Set Eikon's timeout to be 5 min.
# insert APP_KEY from app key generator in eikon
ek.set_app_key("1418cf51ee9046a3a767d6f8c871c1d3fcaf1953")
SIZE = 7200  # Number of rows gathered per Eikon-loop
print(sys.version)
print(ek.__version__)

# WHICH VARIABLE(S) TO RETRIEVE?
# OWN_VAR = 'PeriodEndDate'
OWN_VAR = "PeriodEndDate"
# OWN_TR_VAR = 'TR.F.' + str(OWN_VAR)
OWN_TR_VAR = "TR.F." + str(OWN_VAR)

# FROM WHICH VARIABLE?
# SYM_IN = 'OAPermID'
SYM_IN = 'OrganizationID'

header = [SYM_IN, OWN_VAR]

# YEAR TYPE
YEAR_TYPE = "FY"  # CY or FY (Calendar Year or Fiscal Year)

# SDate?
START_MONTH = "12"  # For rolling Sdates
START_DAY = "31"  # For rolling SDates
FIRST_YEAR = 2000
# LAST_YEAR = datetime.now().year + 1
LAST_YEAR = 2021


# WHERE IS, AND WHERE TO PUT, DATA?
SOURCE_PATH = "D:\\"  # where is?
# SOURCE_PATH = 'C:\\Users\\joach\\OneDrive\\Dokument'  # where is?
OUT_PATH = "D:\\"  # where to?

# FILE NAMES OF DATA
# SOURCE_FNAME = "ric_v2"  # Name of source file
# SOURCE_FNAME = "organizationid_cleaned"  # Name of source file
SOURCE_FNAME = "organizationid_swe"  # Name of source file
SOURCE_FNAME_SUFFIX = ".csv"  # Source file type

OUT_FNAME = str(OWN_VAR).lower()  # Name of output file
OUT_FNAME_SUFFIX = ".csv"  # Output file type

if __name__ == "__main__":

    # PREPARE FILES
    # File name concatenation
    S_FNAME = SOURCE_FNAME + SOURCE_FNAME_SUFFIX
    SOURCE_FNAME_CPL = os.path.join(SOURCE_PATH, S_FNAME)

    O_FNAME = OUT_FNAME + OUT_FNAME_SUFFIX
    OUT_FNAME_CPL = os.path.join(OUT_PATH, O_FNAME)

    # Remove output file, if it exist
    if os.path.exists(OUT_FNAME_CPL):
        os.remove(OUT_FNAME_CPL)

    # Header to output file?
    with open(OUT_FNAME_CPL, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=header)
        writer.writeheader()

    # READ THE DATA FROM SOURCE FILE
    # File has header. make it into a list
    own_list = pd.read_csv(SOURCE_FNAME_CPL, low_memory=False, dtype=str, sep="\t")
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
    print(f"No of {SYM_IN} to retrieve data for: {str(len(own_list))}. "
          f"Variable: {str(OWN_TR_VAR)}")
    for yr in range(FIRST_YEAR, LAST_YEAR + 1):
        # What period? E.g. FY2020, or CY2020
        period = f"{YEAR_TYPE}{yr}"
        # If rolling start date (SDate)? What should it be?
        # s_dte = str(yr) + "-" + str(START_MONTH) + "-" + str(START_DAY)
        s_dte = f"{yr+1}-{START_MONTH}-{START_DAY}"
        # print(" - Period: " + str(period) + ", based on SDate " + str(s_dte))
        print(f" - Period: {period}, based on SDate {s_dte}.")
        own_fields = [
            ek.TR_Field(
                OWN_TR_VAR,
                {
                    "SDate": s_dte,
                    # 'Period': period,
                    # 'RollPeriods': 'False',
                    # 'Scale': '6',s
                    # 'AlignType': 'PeriodEndDate',
                    # 'ReportingState': 'Orig',
                    # 'ReportType': 'Final'
                },
            )
        ]
        # The actual retrieval loop
        # I run this in sections to avoid other types of errors such as 'timeout'
        # errors
        for line_start in range(0, len(own_list) + 1, SIZE):
            line_end = line_start + SIZE
            # Have added a retry loop if error since sometimes there are
            # problems in the API-connection
            for rec_attempts in range(10):
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
                print("All attempts have failed: Program aborted")
                sys.exit()

            # Drop empty rows
            my_header = list(dta.columns.values)
            dta = dta.dropna(how="any", subset=my_header[1])

            # Remove any duplicates
            dta = dta.drop_duplicates()

            # Saves the retrieved Eikon data to out-file
            if len(dta) > 0:
                dta.to_csv(
                    OUT_FNAME_CPL,
                    mode="a",
                    sep="\t",
                    encoding="utf-8",
                    index=False,
                    header=False,
                )
            # Pause for 10s to reduce risk of throwing an exception
            # time.sleep(10)

    # FIX OUTPUT FILE
    # Revised file name
    NEW_O_FNAME = OUT_FNAME + "_v2" + OUT_FNAME_SUFFIX
    NEW_OUT_FNAME_CPL = os.path.join(OUT_PATH, NEW_O_FNAME)

    # Remove revised out-file, if it exists
    if os.path.exists(NEW_OUT_FNAME_CPL):
        os.remove(NEW_OUT_FNAME_CPL)

    # Write header to revised out file
    with open(NEW_OUT_FNAME_CPL, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=header)
        writer.writeheader()

    # Read the original out file
    dta = pd.read_csv(OUT_FNAME_CPL, delimiter="\t", low_memory=False, dtype=str)
    print(f"Length of the original file is {len(dta)}.")

    # Drop empty rows
    dta.dropna(subset=[OWN_VAR], inplace=True)

    # Remove any duplicates
    dta.drop_duplicates(inplace=True)
    print(f"Length of the cleansed file is now {len(dta)}.")

    # Save data
    dta.to_csv(
        NEW_OUT_FNAME_CPL,
        mode="a",
        sep="\t",
        encoding="utf-8",
        index=False,
        header=False,
        quoting=csv.QUOTE_ALL
    )
    print("DONE")
