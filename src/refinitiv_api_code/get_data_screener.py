"""
Created on 26 nov. 2021
@author: joachim landström

This is an application of Refinitiv Screener.
It seeks out firms (Business Organizations) that have either its
headquarter in SE, or that is has its legal place in Sweden.
"""

# IMPORT PACKAGES

import csv
import os
import sys
import time  # For sleep functionality

import eikon as ek  # the Eikon Python wrapper package
import pandas as pd

# SET THE EIKON CONFIGURATION
ek.set_timeout(300)  # Set Eikon's timeout to be 5 min.
# insert APP_KEY from app key generator in eikon
ek.set_app_key("1418cf51ee9046a3a767d6f8c871c1d3fcaf1953")
# SIZE = 7200  # Number of rows gathered per Eikon-loop
print(sys.version)
print(ek.__version__)


OUT_PATH = "D:\\"  # where to?
OUT_FNAME_CPL = os.path.join(OUT_PATH, "test.csv")

# Remove out-file, if it exist
if os.path.exists(OUT_FNAME_CPL):
    os.remove(OUT_FNAME_CPL)

# COMBINATIONS
# active, public
# inactive, public
# active, private
# inactive, private
# Selection criteria:
#     HQ in Sweden OR RegCountry Sweden, AND
#     OrgType is Business Organization (COM)
# Combos:
#   inactive, private
#   inactive, public
#   active, private
#   inactive, private
status = ["active", "inactive"]
pubpriv = ["public", "private"]

dta = pd.DataFrame()
for s in status:
    for p in pubpriv:
        print(f"Screening for {s} and {p} firms")
        df, err = ek.get_data(
            instruments=f"SCREEN(U(IN(Equity({s},{p}))/*UNV:PublicPrivate*/), IN(TR.HQCountryCode,""SE"") OR IN(TR.RegCountryCode,""SE""), IN(TR.OrgTypeCode,""COM""), CURN=SEK)",fields=["TR.OrganizationID"],
        )
        # Saves the retrieved Eikon data to out-file
        # Remove out-file, if it exist
        # if os.path.exists(OUT_FNAME_CPL):
        #     os.remove(OUT_FNAME_CPL)
        OUT_FNAME_CPL = os.path.join(OUT_PATH, f"{s}_{p}.csv")
        df.to_csv(
            OUT_FNAME_CPL,
            mode="w",
            sep="\t",
            encoding="utf-8",
            index=False,
            header=True,
        )
        df = df.drop("Instrument", axis="columns")
        df = df.rename(columns={"Organization PermID": "OrganizationID"})
        dta = dta.append(df)  # TODO Replace with pd.concat(df1, df2, sort=False)
dta = dta.drop_duplicates()
OUT_FNAME_CPL = os.path.join(OUT_PATH, "organizationid_swe.csv")
df.to_csv(
    OUT_FNAME_CPL,
    mode="w",
    sep="\t",
    encoding="utf-8",
    index=False,
    header=True,
)

# df, err = ek.get_data(
#     # instruments="SCREEN(U(IN(Equity(inactive,private))/*UNV:PublicPrivate*/), IN(TR.HQCountryCode,""SE"") OR IN(TR.RegCountryCode,""SE"") OR IN(TR.ExchangeCountryCode,""SE""), CURN=USD)",
#     instruments="SCREEN(U(IN(Equity(inactive,private))/*UNV:PublicPrivate*/), IN(TR.HQCountryCode,""SE"") OR IN(TR.RegCountryCode,""SE""), IN(TR.OrgTypeCode,""COM""), CURN=USD)",
#     fields=['TR.OrganizationID'])

# # Saves the retrieved Eikon data to out-file
# df.to_csv(OUT_FNAME_CPL, mode='a', sep='\t',
#           encoding='utf-8', index=False, header=True)

print(len(df))
print("Done")
