"""
Created on 26 nov. 2021
@author: joachim landström

This file retrieves time series data.

It's main use is to collect stock market data.



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
from datetime import datetime as dt
from src.my_functions import own_functions as own

import eikon as ek  # the Eikon Python wrapper package
import pandas as pd

# SET THE EIKON CONFIGURATION
ek.set_timeout(1000)  # Set Eikon's timeout to be 5 min.
# insert APP_KEY from app key generator in eikon
ek.set_app_key("1418cf51ee9046a3a767d6f8c871c1d3fcaf1953")
SIZE = 100  # Number of rows gathered per Eikon-loop

# SET PANDAS CONFIGURATION
pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)


# Financial instruments to retrieve data for
## Only OrganizationID that has an InstrumentID meeting these restrictions
share_code = ["ORD", "PRF", "FULLPAID", "PREFERRED", "ADR"]

SYM_IN = "OrganizationID"

# FILE NAMES OF DATA
# SOURCE_FNAME = "ric_v2"  # Name of source file
# SOURCE_FNAME = "organizationid_swe"  # Name of source file
# SOURCE_FILE = pl.Path(r"F:\organizationid_test.csv")
proj_path = pl.Path(r"D:")
download_path = pl.Path(r"C:\Users\joach\Downloads")
raw_path = proj_path.joinpath("raw")
out_path = proj_path.joinpath("out")


# FILE NAMES OF DATA
name = "refinitiv_relations.csv"
SOURCE_FILE = pl.Path.joinpath(raw_path, name)
instrument_types = pl.Path.joinpath(raw_path, "instrumenttypecode.csv")
# ipo_dates = pl.Path.joinpath(raw_path, "ipodate_v2.csv")
# priceclose_date = pl.Path.joinpath(raw_path, "priceclosedate_quoteid.csv")
out_file = pl.Path.joinpath(out_path, "fundamentals_interim_date_range.csv")
err_file = pl.Path.joinpath(raw_path, "no_data_organizationid.csv")
# test1 = pl.Path.joinpath(out_path, "test1.csv")
# test2 = pl.Path.joinpath(out_path, "test2.csv")
# test3 = pl.Path.joinpath(out_path, "test3.csv")

# Date range
first_date = "1990-01-01"
last_date = "2022-06-06"

if __name__ == "__main__":

    # PREPARE FILES
    # Remove output file, if it exist
    # header = [
    #     # SYM_IN,
    #     "QuoteID",
    #     "firstdt",
    #     "lastdt",
    #
    # ]
    #
    # # Header to output file?
    # with open(out_file, "w", encoding="UTF8", newline="") as f:
    #     writer = csv.DictWriter(f, delimiter="\t", fieldnames=header)
    #     writer.writeheader()

    # READ THE DATA FROM SOURCE FILE
    # File has header. make it into a list
    own_list = own.read_csv_file(
        SOURCE_FILE, parse_dates=["FirstTradeDate", "RetireDate"]
    )
    own_list = own_list.dropna(
        how="any",
        subset=[
            "OrganizationID",
            "InstrumentID",
        ],  # Must be possible to trace Instrument to Organization
    )

    # Read already collected ID vars
    existing_id = own.read_csv_file(out_file)
    existing_id = existing_id["OrganizationID"]
    existing_id = existing_id.to_frame()
    err_instrument = own.read_csv_file(err_file)
    frames = [existing_id, err_instrument]
    existing_id = pd.concat(frames)
    existing_id.drop_duplicates(inplace=True)
    # own.save_to_csv_file(existing_id, test3, header=True, mode="w")

    ## Subset to particular types financial instruments
    instrumentid = own_list["InstrumentID"]
    instrumentid.dropna(inplace=True)
    instrumentid.drop_duplicates(inplace=True)
    instrumentcodes = own.read_csv_file(instrument_types)
    instrumentcodes.dropna(inplace=True)
    instrumentcodes.drop_duplicates(inplace=True)
    instrumentcodes = instrumentcodes.merge(
        instrumentid, how="inner", on="InstrumentID"
    )

    instruments = pd.DataFrame()  # These are the instruments meeting the instrument-type restriction
    for x in share_code:
        dta = instrumentcodes[instrumentcodes.InstrumentTypeCode == x]
        frames = [instruments, dta]
        instruments = pd.concat(frames)

    own_list = own_list.merge(
        instruments, how="inner", on="InstrumentID"
    )  # Only keep the instruments that meet the above restriction
    own_list = own_list.dropna(
        how="any",
        subset=[
            "OrganizationID",
        ],  # Must be possible to trace Instrument to Organization
    )

    ## Drop if data has already been collected
    own_list = (own_list.merge(existing_id, on="OrganizationID", how="left", indicator=True)
                        .query('_merge == "left_only"')
                        .drop(labels="_merge", axis=1))
    own_list.drop_duplicates(subset=["OrganizationID"], inplace=True)
    own_list = own_list[SYM_IN].values.tolist()

    # own_list = own_list["OrganizationID"]

    # RETRIEVE DATA FROM EIKON
    print(f"No of 'OrganizationID' to retrieve data for: {str(len(own_list))}.")
    # dta_all = pd.DataFrame()  # Just so it is defined
    counter = 0
    for line_start in range(0, len(own_list) + 1, SIZE):
        line_end = line_start + SIZE
        no_data = pd.DataFrame()
        my_list = own_list[line_start:line_end]
        # sdate = date_dict[qte]["SDate"]  # Start date
        # edate = date_dict[qte]["EDate"]  # End date
        sdate = first_date
        edate = last_date

        dta_all = pd.DataFrame()
        for frequency in ["FI","FS","FQ","FY"]:
            print(f" + Lines: {str(line_start)}/{str(line_end)} for frequency {frequency}")
            # Period
            period = f"{frequency}0"  # E.g. FI0 as in current Fiscal Interim
            own_dict = {
                "Period": period,
                "SDate": sdate,
                "Edate": edate,
                "Frq": frequency,
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
                ek.TR_Field("TR.TotalAssetsReported.periodenddate", own_dict)
            ]
            # The actual retrieval loop
            # I run this in sections to avoid other types of errors such as 'timeout'
            # errors

            # Have added a retry loop if error since sometimes there are
            # problems in the API-connection
            for rec_attempts in range(20):
                try:
                    # Retrieval function get_data
                    dta, err = ek.get_data(
                        instruments=my_list,
                        fields=own_fields,
                        field_name=False,
                        raw_output=False,
                    )
                    dta.rename(
                        {
                            "Instrument": "OrganizationID",
                            "Period End Date": "PeriodEndDate"
                        },
                        axis=1,
                        inplace=True,
                    )
                    frames = [dta_all, dta]
                    dta_all = pd.concat(frames)
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
                print(
                    f"All attempts have failed: Program aborted while running lines: {str(line_start)}/{str(line_end)}"
                )
                sys.exit()
        dta = dta_all.copy()
        if dta is not None:
            if not dta.empty:
                # Drop empty rows
                my_header = list(dta.columns.values)
                dta = dta.dropna(how="any", subset=my_header[1:])
                # Remove any duplicates
                dta = dta.drop_duplicates()
            # Appends the retrieved Eikon data to out-file, unless empty dta
            if not dta.empty:
                D_FORMAT = "%Y-%m-%d"  # E.g. 2020-12-31
                dta["PeriodEndDate"] = pd.to_datetime(dta["PeriodEndDate"], format=D_FORMAT)
                dta["firstdt"] = dta.groupby("OrganizationID")["PeriodEndDate"].transform("min")
                dta["lastdt"] = dta.groupby("OrganizationID")["PeriodEndDate"].transform("max")
                dta["firstdt"] = dta["firstdt"].dt.strftime("%Y-%m-%d")
                dta["lastdt"] = dta["lastdt"].dt.strftime("%Y-%m-%d")
                dta = dta[["OrganizationID", "firstdt", "lastdt"]]
                # Drop empty rows
                my_header = list(dta.columns.values)
                dta = dta.dropna(how="any", subset=my_header[1:])
                dta = dta.drop_duplicates()
                # print(f"     From {dta['firstdtf']} to {dta['lastdt']}")
                # Only save data to file once per loop
                if pl.Path.exists(out_file):
                    own.save_to_csv_file(dta, out_file)
                else:
                    own.save_to_csv_file(dta, out_file, header=True, mode="w")
            else:
                no_data["OrganizationID"] = my_list
                if pl.Path.exists(err_file):
                    own.save_to_csv_file(no_data, err_file)
                else:
                    own.save_to_csv_file(no_data, err_file, header=True, mode="w")
                print(f"     No data")
        time.sleep(1)
    print("DONE")
