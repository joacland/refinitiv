'''
Created on 26 nov. 2021
@author: joachim landström

Based on a list of RICs

Retrieves a single Eikon variable (TR.XXX) and saves it into a single csv-file.
The script generates two files, a -raw- file and a v2 file. The v2 file has data
where empty rows, and possible duplicate rows, are discarded. Use v2.

If another type of Eikon variable is sought, as e.g. following the format
TR.F.XXXX, it is necessary to adapt the code below. See -OWN_VAR_SUFFIX- below.

Beware that the list of RICs expects a heading, then a single empty row.

The output file is prefixed with the variable name in lower case
(and suffixed csv). The csv-file is tab separated.

Input required
---------------
Source file name: Name of file with RICs
Source file path: Where is the list
Variable name: Or variable names if many variables are to be collected
Variable suffix: E.g. 'TR.'
Out file path: Where to put the output file?
'''
# IMPORT PACKAGES
# from datetime import datetime
import csv
import os
import sys
import pathlib as pl
import time  # For sleep functionality
from src.my_functions import own_functions as own

import eikon as ek  # the Eikon Python wrapper package
import pandas as pd

# # Proof of concept. Its possible to download data based on the TR.OrganizationID!
# df, err = ek.get_data(
# instruments = ['4295890008','4295890035'],
# fields = ['TR.RIC', 'TR.CommonName','TR.EBITActReportDate']
# )


# SET THE EIKON CONFIGURATION
ek.set_timeout(300)  # Set Eikon's timeout to be 5 min.
# insert APP_KEY from app key generator in eikon
ek.set_app_key('1418cf51ee9046a3a767d6f8c871c1d3fcaf1953')
SIZE = 6500  # Number of rows gathered per Eikon-loop

# FROM WHICH VARIABLE?
# SYM_IN = "InstrumentID"
SYM_IN = "QuoteID"
# SYM_IN = 'OrganizationID'

# WHICH VARIABLES TO RETRIEVE?
# Instrument ID is the Instruments Permanent Identifier
# QuoteID is Eikon's internal unique permanent quote identifier
# OWN_VARIABLES = ["IsPrimaryInstrument", "InstrumentTypeCode" ]  # For InstrumentID
# OWN_VARIABLES = ["IsPrimaryQuote", "ExchangeName", "ExchangeCountryCode", "ShareClass", "RetireDate"]   # For QuoteID
OWN_VARIABLES = ["ShareClass", "RetireDate"]   # For QuoteID
# OWN_VARIABLES = ['IsPrimaryQuote']
# OWN_VARIABLES = ['CommonName']
# OWN_VARIABLES = ['OrganizationID', 'InstrumentID', 'QuoteID']
# OWN_VARIABLES = ['IPODate', 'FirstTradeDate',
#                  'CompanyPublicSinceDate', 'RetireDate']
# OWN_VARIABLES = ['OrganizationID', 'LegalEntityIdentifier']

# Suffix to variables
OWN_VAR_SUFFIX = "TR."

# WHERE IS, AND WHERE TO PUT, DATA?
# SOURCE_PATH = 'C:\\Users\\joach\\OneDrive\\Dokument'
proj_path = pl.Path(r"D:")
raw_path = proj_path.joinpath("raw")
OUT_PATH = 'D:\\'  # where to?
out_path = proj_path.joinpath("out")

# FILE NAMES OF DATA
name = "refinitiv_relations.csv"
SOURCE_FILE = pl.Path.joinpath(raw_path, name)

if __name__ == '__main__':

    # READ THE DATA FROM SOURCE FILE
    # File has header. make it into a list
    own_list = own.read_csv_file(SOURCE_FILE, dtype=str)
    # If SYM_IN is not own column, slice it and drop duplicates
    own_list = own_list[[SYM_IN]]
    # Find RIC-series delisted, strip delisting info, and append to original
    if SYM_IN == "RIC":
        delisted = own_list[own_list.RIC.str.contains('\^[A-Z][0-9]{2}')]
        delisted_original = delisted["RIC"].str.replace(r'\^[A-Z][0-9]{2}', '', regex=True, case=True)
        delisted_original = delisted_original.to_frame()
        delisted_original = delisted_original.drop_duplicates()
        own_list = own_list.append(delisted_original)
    own_list = own_list.drop_duplicates()
    own_list = own_list.dropna()
    own_list = own_list[SYM_IN].values.tolist()

    # RETRIEVE DATA FROM EIKON
    print(f'No of {SYM_IN}s to retrieve data for: {str(len(own_list))}')
    for i in OWN_VARIABLES:
        OWN_VAR = str(OWN_VAR_SUFFIX) + i
        print(f'Retrieves data for Eikon variable: {str(OWN_VAR)}')

        # Out file name concatenation
        O_FNAME = str(i).lower() + '.csv'
        OUT_FNAME_CPL = pl.Path.joinpath(out_path, O_FNAME)

        # Remove out-file, if it exist
        if os.path.exists(OUT_FNAME_CPL):
            os.remove(OUT_FNAME_CPL)

        # Header to output file?
        header = [SYM_IN, i]
        with open(OUT_FNAME_CPL, 'w', encoding='UTF8', newline='') as f:
            writer = csv.DictWriter(f, delimiter='\t', fieldnames=header)
            writer.writeheader()

        # The actual retrieval loop
        # I run this in sections to avoid other types of errors such as 'timeout'
        # errors
        for line_start in range(0, len(own_list) + 1, SIZE):
            line_end = line_start + SIZE
            print(' - From ' + str(line_start) + ', to ' + str(line_end))
            my_fields = OWN_VAR

            # Have added a retry loop if error since sometimes there are
            # problems in the API-connection
            for rec_attempts in range(20):
                try:
                    dta, err = ek.get_data(
                        instruments=own_list[line_start:line_end],
                        fields=my_fields
                    )
                except Exception as own_err:
                    print('Exception in attempt #' + str(rec_attempts) +
                          ': ' + str(own_err) + ', was raised. Trying again.')
                    # Try again after a sleep of 30 seconds
                    time.sleep(30)
                    continue
                else:
                    break
            else:
                # All attempts failed
                print('All attempts have failed: Program aborted while \
                processing variable' + str(my_fields))
                sys.exit()

            # Saves the retrieved Eikon data to out-file
            dta.to_csv(OUT_FNAME_CPL, mode='a', sep='\t', float_format='str',
                       quoting=csv.QUOTE_ALL, encoding='utf-8',
                       index=False, header=False)
            # Pause for 10s to reduce risk of throwing an exception
            # time.sleep(10)

    # FIX OUTPUT FILES
    for o_var in OWN_VARIABLES:
        print('Final process for ' + str(o_var))
        # Out-file name concatenation
        # Original file
        O_FNAME = str(o_var).lower() + '.csv'
        OUT_FNAME_CPL = os.path.join(OUT_PATH, O_FNAME)
        # Revised file
        NEW_O_FNAME = str(o_var).lower() + '_v2' + '.csv'
        NEW_OUT_FNAME_CPL = os.path.join(OUT_PATH, NEW_O_FNAME)

        # Remove revised out-file, if it exists
        if os.path.exists(NEW_OUT_FNAME_CPL):
            os.remove(NEW_OUT_FNAME_CPL)

        # Write header to revised out file
        header = [SYM_IN, o_var]
        with open(NEW_OUT_FNAME_CPL, 'w', encoding='UTF8', newline='') as f:
            writer = csv.DictWriter(f, delimiter='\t', fieldnames=header)
            writer.writeheader()
        # Read the original out file
        dta = pd.read_csv(OUT_FNAME_CPL, delimiter='\t', low_memory=False, dtype=str)
        print(len(dta))

        # Drop empty rows
        dta = dta.dropna(subset=[o_var])

        # Remove any duplicates
        dta = dta.drop_duplicates()
        print(len(dta))

        # Save data
        dta.to_csv(NEW_OUT_FNAME_CPL, mode='a', sep='\t',
                   quoting=csv.QUOTE_ALL, encoding='utf-8',
                   index=False, header=False)
    print('DONE')
