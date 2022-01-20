"""
Created on 26 nov. 2021
@author: joachim landström

Based on a list of RICs

Retrieves a single Eikon instrument identifier and saves it into a single csv-file.
The script generates two files, a -raw- file and a v2 file. The v2 file has data
where empty rows, and possible duplicate rows, are discarded. Use v2.

Beware that the list of RICs does not expect a heading, and that its a pure txt-file.

The output file is prefixed with the variable name in lower case
(and suffixed csv). The csv-file is tab separated.

TODO: This needs to be fully converted to save diction as JSON since 
raw_output = True gives dictions and setting best_match = False gives too strange
output as a Pandas dataframe. I've started the work, but its not complete. 
Something to think of is how to append a diction as the loop goes on. It's not
valid to append a JSON file with dictions. Figure out appending dictions

Input required
---------------
Source file name: Name of file with RICs
Source file path: Where is the list
Out file path: Where to put the output file?
"""
# IMPORT PACKAGES
#from datetime import datetime
import csv
import json5
import os
import sys
import time  # For sleep functionality

import eikon as ek  # the Eikon Python wrapper package
import pandas as pd


def e_get_symbols(sym_lst, sym_in, sym_out):
    """
    Function to retrieve symbol data from Eikon.
    Input symbol (from list) is sym_in
    Output symbol if sym_out
    best_match is set to False to get all matches and not just Primary
    """
    sym_df = ek.get_symbology(
        sym_lst,
        from_symbol_type=sym_in,
        to_symbol_type=sym_out,
        best_match=False,
        raw_output=True
    )
    return sym_df


def merge_two_dicts(x, y):
    """Given two dictionaries, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z


if __name__ == '__main__':
    # SET THE EIKON CONFIGURATION
    ek.set_timeout(300)  # Set Eikon's timeout to be 5 min.
    # insert APP_KEY from app key generator in eikon
    ek.set_app_key('1418cf51ee9046a3a767d6f8c871c1d3fcaf1953')
    SIZE = 2  # Number of rows gathered per Eikon-loop

    # WHICH VARIABLES TO RETRIEVE?
    #OWN_VARIABLES = ['ISIN', 'SEDOL', 'CUSIP', 'OAPermID']
    OWN_VARIABLES = ['ISIN']
    # FROM WHICH VARIABLE?
    SYM_IN = 'RIC'

    # WHERE IS, AND WHERE TO PUT, DATA?
    SOURCE_PATH = 'C:\\Users\\joach\\OneDrive\\Dokument'  # where is?
    OUT_PATH = 'G:\\'  # where to?

    # FILE NAMES OF DATA
    SOURCE_FNAME = 'ric_all'  # Name of source file
    SOURCE_FNAME_SUFFIX = '.csv'  # File type
    # Out file specified in loop below

    # Source file name concatenation
    S_FNAME = SOURCE_FNAME + SOURCE_FNAME_SUFFIX
    SOURCE_FNAME_CPL = os.path.join(SOURCE_PATH, S_FNAME)

    # READ THE DATA FROM SOURCE FILE
    # File has header. make it into a list
    own_list = pd.read_csv(SOURCE_FNAME_CPL, low_memory=False)
    own_list = own_list['ric'].values.tolist()
    own_list = ['VOLVb.ST', 'ATCOa.ST', 'HUFVa.ST',
                'VOLO.ST', '247.TE', '24STOR.ST']
    #own_list = own_list[1:500]

    # RETRIEVE DATA FROM EIKON
    print('No of RICs to retrieve data for: ' + str(len(own_list)))
    for i in OWN_VARIABLES:
        OWN_VAR = i
        print('Retrieves data for Eikon variable: ' + str(OWN_VAR))

        # Out file name concatenation
        O_FNAME = str(i).lower() + '.json'
        OUT_FNAME_CPL = os.path.join(OUT_PATH, O_FNAME)
        print(OUT_FNAME_CPL)

        # Remove out-file, if it exist
        if os.path.exists(OUT_FNAME_CPL):
            os.remove(OUT_FNAME_CPL)

        # Write header to out-file
        # header = ['ric', OWN_VAR]
        # with open(OUT_FNAME_CPL, 'w', encoding='UTF8') as f:
        #     writer = csv.writer(f, delimiter='\t')
        #     writer.writerow(header)

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
                    dta = e_get_symbols(own_list[j:k], SYM_IN, OWN_VAR)
                    new_name = 'mappedSymbols' + '_' + str(j)
                    dta[new_name] = dta.pop('mappedSymbols')
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
                print('All attempts have failed: Program aborted')
                sys.exit()

            # Append json array to json file
            if j == 0:
                with open(OUT_FNAME_CPL, 'w') as fp:
                    json5.dump(dta, fp, sort_keys=True,
                               indent=4, trailing_commas=False)
            else:
                output_list = []
                with open(OUT_FNAME_CPL, 'r') as fp:
                    output_list.append(json5.load(fp))

                with open(OUT_FNAME_CPL, 'w') as fp:
                    json5.dump(dta, fp, sort_keys=True,
                               indent=4, trailing_commas=False)

            # Saves the retrieved Eikon data to out-file
            # dta.to_csv(OUT_FNAME_CPL, mode='a', sep='\t',
            #            encoding='utf-8', index=False, header=False)
            # Pause for 10s to reduce risk of throwing an exception
            time.sleep(10)
        print('Time to dump')
        # with open(OUT_FNAME_CPL, 'w', encoding="utf-8") as fp:
        #     json5.dump(own_dict, fp, sort_keys=True,
        #                indent=4, trailing_commas=False)

    # # FIX OUTPUT FILES
    # for i in OWN_VARIABLES:
    #     # Out-file name concatenation
    #     # Original file
    #     O_FNAME = str(i).lower() + '.csv'
    #     OUT_FNAME_CPL = os.path.join(OUT_PATH, O_FNAME)
    #     # Revised file
    #     NEW_O_FNAME = str(i).lower() + '_v2' + '.csv'
    #     NEW_OUT_FNAME_CPL = os.path.join(OUT_PATH, NEW_O_FNAME)
    #
    #     # Remove revised out-file, if it exists
    #     if os.path.exists(NEW_OUT_FNAME_CPL):
    #         os.remove(NEW_OUT_FNAME_CPL)
    #
    #     # Write header to revised out file
    #     header = ['ric', i]
    #     with open(NEW_OUT_FNAME_CPL, 'w', encoding='UTF8') as f:
    #         writer = csv.writer(f, delimiter='\t')
    #         writer.writerow(header)
    #
    #     # Read the original out file
    #     dta = pd.read_csv(OUT_FNAME_CPL, delimiter='\t',
    #                       low_memory=False, skiprows=[2])
    #     print(len(dta))
    #
    #     # Drop empty rows
    #     dta = dta.dropna(how='any', subset=[i])
    #
    #     # Remove any duplicates
    #     dta.drop_duplicates(inplace=True)
    #     print(len(dta))
    #
    #     # Save data
    #     dta.to_csv(NEW_OUT_FNAME_CPL, mode='a', sep='\t',
    #                encoding='utf-8', index=False, header=False)
    print('DONE')
