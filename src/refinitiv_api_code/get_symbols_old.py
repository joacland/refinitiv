'''
Created on 26 nov. 2021
@author: joachim landström

Based on a list of RICs

Retrieves a single Eikon instrument identifier and saves it into a single csv-file.

The output is too mangled and needs manual fixing. Data sometimes comes out
in more than two columns and the order of the columns may then change.
Requires manual adjustment

The output file is prefixed with the variable name in lower case
(and suffixed csv). The csv-file is tab separated.

All data is enclosed in "".

Input required
---------------
Source file name: Name of file with RICs
Source file path: Where is the list
Out file path: Where to put the output file?
'''
# IMPORT PACKAGES
# from datetime import datetime
import csv
import os
import time  # For sleep functionality

import eikon as ek  # the Eikon Python wrapper package
import pandas as pd


def e_get_symbols(sym_lst, sym_in, sym_out):
    '''
    Function to retrieve symbol data from Eikon.
    Input symbol (from list) is sym_in
    Output symbol if sym_out
    best_match is set to False to get all matches and not just Primary
    '''
    sym_df = ek.get_symbology(
        sym_lst,
        from_symbol_type=sym_in,
        to_symbol_type=sym_out,
        best_match=True
    )
    return sym_df


if __name__ == '__main__':
    # SET THE EIKON CONFIGURATION
    ek.set_timeout(300)  # Set Eikon's timeout to be 5 min.
    # insert APP_KEY from app key generator in eikon
    ek.set_app_key('1418cf51ee9046a3a767d6f8c871c1d3fcaf1953')
    SIZE = 7200  # Number of rows gathered per Eikon-loop

    # WHICH VARIABLES TO RETRIEVE?
    # OWN_VARIABLES = ['OAPermID', 'ISIN', 'SEDOL', 'CUSIP']
    OWN_VARIABLES = ['OAPermID', 'ISIN']
    # OWN_VARIABLES = ['RIC']
    # OWN_VARIABLES = ['OAPermID']

    # FROM WHICH VARIABLE?
    # SYM_IN = 'OAPermID'
    SYM_IN = 'RIC'

    # WHERE IS, AND WHERE TO PUT, DATA?
    SOURCE_PATH = "G:\\"  # where is?
    # SOURCE_PATH = 'C:\\Users\\joach\\OneDrive\\Dokument'  # where is?
    OUT_PATH = 'G:\\'  # where to?

    # FILE NAMES OF DATA
    # SOURCE_FNAME = 'ric_all'  # Name of source file
    # SOURCE_FNAME = "OAPermID_cleaned"  # Name of source file
    SOURCE_FNAME = "rics_cleaned_v2"  # Name of source file
    SOURCE_FNAME_SUFFIX = '.csv'  # File type
    # Out file specified in loop below

    # Source file name concatenation
    S_FNAME = SOURCE_FNAME + SOURCE_FNAME_SUFFIX
    SOURCE_FNAME_CPL = os.path.join(SOURCE_PATH, S_FNAME)

    # READ THE DATA FROM SOURCE FILE
    # File has header. make it into a list
    own_list = pd.read_csv(SOURCE_FNAME_CPL, low_memory=False, dtype=str, sep="\t")
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
    own_list = own_list[SYM_IN].values.tolist()

    # RETRIEVE DATA FROM EIKON
    print(f'No of {SYM_IN}s to retrieve data for: {str(len(own_list))}')
    for i in OWN_VARIABLES:
        OWN_VAR = i
        print('Retrieves data for Eikon variable: ' + str(OWN_VAR))

        # Out file name concatenation
        O_FNAME = str(OWN_VAR).lower() + '_symbology.csv'
        OUT_FNAME_CPL = os.path.join(OUT_PATH, O_FNAME)
        print('Data saved to file ' + str(OUT_FNAME_CPL))

        # Remove out-file, if it exist
        if os.path.exists(OUT_FNAME_CPL):
            os.remove(OUT_FNAME_CPL)

        # Write header to out-file
        header = [SYM_IN, OWN_VAR]
        with open(OUT_FNAME_CPL, 'w', encoding='UTF8', newline='') as f:
            writer = csv.DictWriter(f, delimiter='\t', fieldnames=header)
            writer.writeheader()

        # The actual retrieval loop
        # I run this in sections to avoid other types of errors such as 'timeout'
        # errors
        for j in range(0, len(own_list), SIZE):
            k = j + SIZE
            print(' - From ' + str(j) + ', to ' + str(k))

            # Have added a retry loop if error since sometimes there are
            # problems in the API-connection
            for rec_attempts in range(5):
                try:
                    # Retrieve symbol data from Eikon
                    dta = e_get_symbols(
                        own_list[j:k], SYM_IN, [OWN_VAR, SYM_IN])
                except Exception as own_err:
                    print('Exception in attempt #' + str(rec_attempts)
                          + ': ' + str(own_err) + ', was raised. Trying again.')
                    # Try again after a sleep of 30 seconds
                    time.sleep(30)
                    continue
                else:
                    break
            else:
                # All attempts failed
                print('All attempts have failed: Program aborted')
                sys.exit()

                # Saves the retrieved Eikon data to out-file
            dta.to_csv(OUT_FNAME_CPL, mode='a', sep='\t',
                       quoting=csv.QUOTE_ALL, encoding='utf-8',
                       index=False, header=False)
            # Pause for 10s to reduce risk of throwing an exception
            time.sleep(10)
print('DONE')
