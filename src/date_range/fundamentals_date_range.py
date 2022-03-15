"""
This script identifies the first and last fiscal year for which an
OrganizationID has data in Refinitiv Fundamental (which is different from
Refinitiv Company Fundamentals). Refinitiv Fundamentals is originonally
Reuter's Fundamentals.

"""
import pathlib as pl
import pandas as pd
import csv
from src.my_functions import own_functions as own

# SET PANDAS CONFIGURATION
pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)

# WHERE IS, AND WHERE TO PUT, DATA?
SOURCE_FILE = pl.Path(r"F:\bsperiodenddate_v2.csv")  # where is?
OUT_FILE = pl.Path(r"F:\fundamentals_date_range.csv")  # where to?

if __name__ == "__main__":
    dta = own.read_csv_file(SOURCE_FILE)
    dta = dta.rename(columns={"BSPeriodEndDate": "PeriodEndDate"})
    D_FORMAT = "%Y-%m-%d"  # E.g. 2020-12-31
    dta["PeriodEndDate"] = pd.to_datetime(dta["PeriodEndDate"], format=D_FORMAT)
    dta["year"] = dta["PeriodEndDate"].dt.year

    dta["year1"] = dta.groupby("OrganizationID")["year"].transform("min")
    dta["year2"] = dta.groupby("OrganizationID")["year"].transform("max")

    dta = dta[["OrganizationID", "year1", "year2"]]
    dta.drop_duplicates(inplace=True)

    with open(OUT_FILE, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(
            f, delimiter="\t", fieldnames=dta.columns.values
        )
        writer.writeheader()

    own.save_to_csv_file(dta, OUT_FILE, mode="a")
