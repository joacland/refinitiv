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
import time  # For sleep functionality

import eikon as ek  # the Eikon Python wrapper package
import pandas as pd

# SET THE EIKON CONFIGURATION
ek.set_timeout(300)  # Set Eikon's timeout to be 5 min.
# insert APP_KEY from app key generator in eikon
ek.set_app_key('1418cf51ee9046a3a767d6f8c871c1d3fcaf1953')
SIZE = 7200  # Number of rows gathered per Eikon-loop
print(sys.version)
print(ek.__version__)


OUT_PATH = 'G:\\'  # where to?
OUT_FNAME_CPL = os.path.join(OUT_PATH, 'test.csv')

# Remove out-file, if it exist
if os.path.exists(OUT_FNAME_CPL):
    os.remove(OUT_FNAME_CPL)

# COMBINATIONS
# active, public
# inactive, public
# active, private
# inactive, private

df, err = ek.get_data(
    instruments="SCREEN(U(IN(Equity(inactive,private))/*UNV:PublicPrivate*/), IN(TR.HQCountryCode,""SE"") OR IN(TR.RegCountryCode,""SE"") OR IN(TR.ExchangeCountryCode,""SE""), CURN=USD)",
    fields=['TR.OrganizationID'])

# Saves the retrieved Eikon data to out-file
df.to_csv(OUT_FNAME_CPL, mode='a', sep='\t',
          encoding='utf-8', index=False, header=True)

print(len(df))
print('Done')
