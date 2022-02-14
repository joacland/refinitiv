"""
This script identifies Swedish-traded firms (OrganizationID) based on RIC-suffixes

Valid RIC suffixes are:
    .ST     Nasdaq OMX, FirstNorth, and some Spotlight
    .TE     Spotlight
    .NGN    NGM

It also identifies all firms bearing a relation to Sweden according to screener.
TR.HQCountryCode is "SE", or TR.RegCountryCode is "SE",
or TR.ExchangeCountryCode is "SE".

The script requires three files:
    In box/data:
    * ric_organizationid.csv

    In directory 'raw' in project:
    active_public.csv
    active_private.csv
    inactive_public.csv
    inactive_private.csv
    (these 4 files can be updated using get_data_screener.py)
"""
import pandas as pd
import pathlib as pl
import os
import cs
from pandas_ods_reader import read_ods
from src.my_functions import own_functions as own

if __name__ == "__main__":
    # PROJECT DIRECTORY
    PROJ_PATH = pl.Path.home().joinpath("Documents", "research", "refinitiv")

    # NAME OF SOURCE AND OUTPUT DIRECTORY (INSIDE PROJECT DIRECTORY FROM ABOVE)
    OUT_DIR = "out"
    RAW_DIR = "raw"

    # RAW FILE NAME
    RAW_MRK = pl.Path.home().joinpath("box", "data", "ric_organizationid.csv")

    # OUT FILE NAME OF DATA
    OUT_PATH_MRK = PROJ_PATH.joinpath(
        OUT_DIR, "swe_mrk_organizationid.csv"
    )  # Out path for outfile
    OUT_PATH_ALL = PROJ_PATH.joinpath(
        OUT_DIR, "swe_all_organizationid.csv"
    )  # Out path for outfile

    # Remove output file, if it exists
    if os.path.exists(OUT_PATH_MRK):
        os.remove(OUT_PATH_MRK)
    if os.path.exists(OUT_PATH_ALL):
        os.remove(OUT_PATH_ALL)

    # Get OrganizationID for firms having been, or is being traded in Sweden
    df = own.read_csv_file(RAW_MRK)
    swe_market = ["ST", "TE", "NGM"]
    swe_mrk = pd.DataFrame()
    for mkr in swe_market:
        sel = f"\.{mkr}"
        partial = df[df.RIC.str.contains(sel, regex=True)]
        partial = partial[["OrganizationID"]]
        swe_mrk = swe_mrk.append(partial)
    swe_mrk = swe_mrk.drop_duplicates()
    swe_mrk = swe_mrk.sort_values(by="OrganizationID", na_position="last")
    own.save_to_csv_file(
        swe_mrk, OUT_PATH_MRK, float_format=str, quoting=csv.QUOTE_NONNUMERIC
    )

    # Get OrganizationID for Swedish firms
    # (public & private, active or non-active)
    files = [
        "active_public",
        "active_private",
        "inactive_public",
        "inactive_private",
    ]
    swe_all = pd.DataFrame()
    for file in files:
        f = f"{file}.csv"
        RAW_ALL = pl.Path.home().joinpath(PROJ_PATH, RAW_DIR, f)
        df = own.read_csv_file(RAW_ALL)
        df = df.rename(columns={"Organization PermID": "OrganizationID"})
        df = df[["OrganizationID"]]
        swe_all = swe_all.append(df)
    swe_all = swe_all.drop_duplicates()
    swe_all = swe_all.sort_values(by="OrganizationID", na_position="last")
    own.save_to_csv_file(
        swe_all, OUT_PATH_ALL, float_format=str, quoting=csv.QUOTE_NONNUMERIC
    )
