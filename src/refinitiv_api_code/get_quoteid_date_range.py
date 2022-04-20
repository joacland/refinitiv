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
SIZE = 1500  # Number of rows gathered per Eikon-loop

# SET PANDAS CONFIGURATION
pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)


# Financial instruments to retrieve data for
share_code = ["ORD", "PRF", "FULLPAID", "PREFERRED", "ADR"]


FIRST_YEAR = 1999
# LAST_YEAR = datetime.now().year + 1
LAST_YEAR = 2022

SYM_IN = "QuoteID"

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
ipo_dates = pl.Path.joinpath(raw_path, "ipodate_v2.csv")
priceclose_date = pl.Path.joinpath(raw_path, "priceclosedate_v2.csv")
out_file = pl.Path.joinpath(out_path, "quoteid_date_range.csv")
ret_file = pl.Path.joinpath(download_path, "tret.csv")
err_file = pl.Path.joinpath(raw_path, "no_data.csv")
test1 = pl.Path.joinpath(out_path, "test1.csv")
test2 = pl.Path.joinpath(out_path, "test2.csv")
test3 = pl.Path.joinpath(out_path, "test3.csv")


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
            "InstrumentID",
            "QuoteID",
        ],  # Must be possible to trace to Quote and to Organization
    )

    # Read already collected ID vars
    existing_id = own.read_csv_file(out_file)
    existing_id = existing_id["QuoteID"]
    existing_id = existing_id.to_frame()
    err_quote = own.read_csv_file(err_file)
    frames = [existing_id, err_quote]
    existing_id = pd.concat(frames)
    existing_id.drop_duplicates(inplace=True)
    own.save_to_csv_file(existing_id, test3, header=True, mode="w")

    # Add IPO Dates
    ipo = own.read_csv_file(ipo_dates)
    ipo = ipo.dropna(how="any", subset=["IPODate"])
    ipo.drop_duplicates(subset=["RIC"], inplace=True)
    own_list = own_list.merge(ipo, how="left", on="RIC")

    # Add known price close date
    priceclose = own.read_csv_file(priceclose_date)
    priceclose = priceclose.dropna(how="any", subset=["PriceCloseDate"])
    priceclose.drop_duplicates(subset=["QuoteID"], inplace=True)
    own_list = own_list.merge(priceclose, how="left", on="QuoteID")

    # Set SDate and EDate per instrument
    ## First get know dates
    first_date = "1999-01-01"
    last_date = "2022-04-11"
    first_last_dates = own_list.copy()
    first_last_dates = first_last_dates[
        [
            "InstrumentID",
            "QuoteID",
            "FirstTradeDate",
            "RetireDate",
            "IPODate",
            "PriceCloseDate",
        ]
    ]  # Get known dates

    first_last_dates.rename(
        {"FirstTradeDate": "SDate", "RetireDate": "EDate"}, axis=1, inplace=True
    )
    first_last_dates.drop_duplicates(inplace=True)

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

    instruments = (
        pd.DataFrame()
    )  # These are the instruments meeting the instrument-type restriction
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
            "InstrumentID",
            "QuoteID",
        ],  # Must be possible to trace to Quote and to Organization
    )

    ## Set SDate and EDate based on known dates and other restrictions
    first_last_dates["SDate"] = first_last_dates.groupby(
        "QuoteID"
    ).SDate.transform(
        "min"
    )  # Some IPO dates behave strangely. Safety belt before proceeding
    ### Minimum Date (based on FirstTradeDate, which is an issue level descriptive item)
    first_last_dates["SDate"] = first_last_dates["SDate"].fillna(
        first_last_dates["IPODate"]
    )  # Start date not before IPO Date
    first_last_dates["SDate"] = first_last_dates["SDate"].mask(
        first_last_dates["SDate"] > first_last_dates["IPODate"],
        first_last_dates["IPODate"],
    )  # Start date not before IPO Date
    first_last_dates["SDate"] = first_last_dates["SDate"].fillna(
        first_date
    )  # Don't look before first_date, so fill all remaining NA with this date
    first_last_dates["SDate"] = first_last_dates["SDate"].mask(
        first_last_dates["SDate"] < first_date, first_date
    )  # Don't look before first_date, so replace when necessary
    ### Maximum Date (based on RetireDate which is a Quote Descriptive level item)
    first_last_dates["EDate"] = first_last_dates.groupby(
        "QuoteID"
    ).EDate.transform("max")
    first_last_dates["EDate"] = first_last_dates["EDate"].fillna(
        first_last_dates["PriceCloseDate"]
    )
    first_last_dates["EDate"] = first_last_dates["EDate"].mask(
        first_last_dates["EDate"] > first_last_dates["PriceCloseDate"],
        first_last_dates["PriceCloseDate"],
    )  # End date not after PriceCloseDate (last date for closing price)
    first_last_dates["EDate"] = first_last_dates["EDate"].fillna(last_date)
    first_last_dates["EDate"] = first_last_dates["EDate"].mask(
        first_last_dates["EDate"] > last_date, last_date
    )  # Don't look after last_date
    ### Keep only 'non-out-of-bounds' rows -- something is wrong with such rows
    first_last_dates = first_last_dates[
        first_last_dates.EDate > first_last_dates.SDate
    ]  # EDate > SDate
    ### Keep if EDate after first_date
    first_last_dates = first_last_dates[
        first_last_dates.EDate > first_date
    ]  # EDate > first_date
    ### Fix possible non-unique QuoteID to date range min(SDate) -- max(EDate)
    first_last_dates["SDate"] = first_last_dates.groupby(
        "QuoteID"
    ).SDate.transform("min")
    first_last_dates["EDate"] = first_last_dates.groupby(
        "QuoteID"
    ).EDate.transform("max")

    ### Drop if data has already been collected
    first_last_dates = (first_last_dates.merge(existing_id, on="QuoteID", how="left", indicator=True)
                        .query('_merge == "left_only"')
                        .drop(labels="_merge", axis=1))

    ### Convert into a dictionary (and Quotes as a list)
    first_last_dates = first_last_dates[["QuoteID", "SDate", "EDate"]]
    first_last_dates = first_last_dates.sort_values("QuoteID")
    first_last_dates.drop_duplicates(subset=["QuoteID"], inplace=True)

    own_list = first_last_dates.copy()
    own.save_to_csv_file(own_list, test2, header=True, mode="w")
    # own_list = list.dropna(subset=["QuoteID"], inplace=True)
    own_list = (own_list.merge(existing_id, on="QuoteID", how="left", indicator=True)
                        .query('_merge == "left_only"')
                        .drop(labels="_merge", axis=1))
    own_list.drop_duplicates(subset=["QuoteID"], inplace=True)

    own_list = own_list["QuoteID"]
    # own_list = own_list["QuoteID"].values.tolist()

    first_last_dates["SDate"] = first_last_dates["SDate"].dt.strftime(
        "%Y-%m-%d"
    )  # Make into a string to avoid timestamp output in the dictionary
    first_last_dates["EDate"] = first_last_dates["EDate"].dt.strftime(
        "%Y-%m-%d"
    )
    first_last_dates = first_last_dates.set_index("QuoteID")
    date_dict = first_last_dates.to_dict(
        "index"
    )  # Dictionary w/ SDate and EDate per QuoteID

    # RETRIEVE DATA FROM EIKON
    print(f"No of 'QuoteID' to retrieve data for: {str(len(own_list))}.")
    # dta_all = pd.DataFrame()  # Just so it is defined
    counter = 0
    for qte in own_list:
        no_data = pd.DataFrame()
        counter = counter + 1
        sdate = date_dict[qte]["SDate"]  # Start date
        edate = date_dict[qte]["EDate"]  # End date
        # print(" - Period: " + str(period) + ", based on SDate " + str(s_dte))
        print(f" - QuoteID {qte} for period {sdate} -- {edate}. #{counter}/{str(len(own_list))}")
        own_dict = {
            # "Period": period,
            "SDate": sdate,
            "Edate": edate,
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
            ek.TR_Field("TR.PriceCloseDate", own_dict),
            ek.TR_Field("TR.TotalReturn1D", own_dict),
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
                    instruments=qte,
                    fields=own_fields,
                    field_name=False,
                    raw_output=False,
                )
                dta.rename(
                    {
                        "Instrument": "QuoteID",
                        "Date": "date",
                        "Daily Total Return": "tret",
                    },
                    axis=1,
                    inplace=True,
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
            print(
                f"All attempts have failed: Program aborted while running QuoteID {qte}"
            )
            sys.exit()

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
                dta = dta[["QuoteID", "firstdt", "lastdt"]]
                dta = dta.drop_duplicates()
                # print(f"     From {dta['firstdtf']} to {dta['lastdt']}")
                # Only save data to file once per loop
                if pl.Path.exists(out_file):
                    own.save_to_csv_file(dta, out_file)
                else:
                    own.save_to_csv_file(dta, out_file, header=True, mode="w")
            else:
                no_data["QuoteID"] = [qte]
                if pl.Path.exists(err_file):
                    own.save_to_csv_file(no_data, err_file)
                else:
                    own.save_to_csv_file(no_data, err_file, header=True, mode="w")
                print(f"     No data")
        time.sleep(1)
    print("DONE")
