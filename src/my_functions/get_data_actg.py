"""
This script retrieves full IS, BS & CF statements, including some extra info
from Retriever Eikon for a list of OrganizationID. The payload is rather
large since it retrieves e.g. all variables the IS-template for Eikon holds.
This means that the number of IDs must be kept small to avoid e.g. undisclosed
"non-retrieves".Consequently, this scripts takes a long time to process.
set FIRST_YEAR and LAST_YEAR, and add the list file with OrganizationID,
which should have OrganizationID as column head.

"""
import os
import sys
import csv
import time  # For sleep functionality
import eikon as ek
import pandas as pd
from src.my_functions import own_functions as own

# SET PANDAS CONFIGURATION
pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)

# SET THE EIKON CONFIGURATION
ek.set_timeout(300)  # Set Eikon's timeout to be 5 min.
# insert APP_KEY from app key generator in eikon
ek.set_app_key("1418cf51ee9046a3a767d6f8c871c1d3fcaf1953")
SIZE = 5  # Number of IDs gathered per Eikon-loop. This must be very small.
# print(sys.version)
# print(ek.__version__)

# GLOBAL SETTINGS FOR DATA RETRIEVAL
rep_type = "Final"
rep_state = "Orig"
consol_basis = "Primary"
curn = "Native"
align = "PeriodEndDate"
roll = "False"
special = "Yes"

FIRST_YEAR = 2000
LAST_YEAR = 2020

# WHERE IS, AND WHERE TO PUT, DATA?
SOURCE_PATH = "D:\\"  # where is?
OUT_PATH = "D:\\"  # where to?

# FILE NAMES OF DATA
# SOURCE_FNAME = "ric_v2"  # Name of source file
# SOURCE_FNAME = "swe_mrk_organizationid.csv"  # Name of source file
SOURCE_FNAME = "swe_all_organizationid.csv"  # Name of source file
# SOURCE_FNAME = "swe_all_organizationid.csv"  # Name of source file

# WHICH VARIABLE(S) TO RETRIEVE?
header = [
    "OrganizationID",
    "PeriodEndDate",
    "StmtPrelimFlag",  # Activation Date of summary estimate
    "FundConsol",
    "Currency",
    "FundamentalAcctStd",
    "VariableName",
    "VariableValue",
]

if __name__ == "__main__":
    # PREPARE FILES
    SOURCE_FNAME_CPL = os.path.join(SOURCE_PATH, SOURCE_FNAME)

    # READ THE DATA FROM SOURCE FILE
    # File has header. make it into a list
    own_list = own.read_csv_file(SOURCE_FNAME_CPL)
    own_list = own_list["OrganizationID"].values.tolist()
    print("No of OrganizationIDs to retrieve data for: " + str(len(own_list)))
    # own_list = ['4295890024', '4295859652']

    # RETRIEVE DATA FROM EIKON
    # Template names:
    # "IncomeStatement"  # Financial Statement
    # "BalanceSheet"  # Financial Statement
    # "CashFlowStatement" # Financial Statement
    # "FootnotesINC"  # Holds Average No of Employees
    # "FootnotesCAS"  # Holds Dividends to Common
    # "BusinessSegments"  # Holds Business Segment Level Data. Must be retrieved
    # independently of other templates.

    REFINITIV_TEMPLATES = [
        "IncomeStatement",
        "BalanceSheet",
        "CashFlowStatement",
        "FootnotesINC",
        "FootnotesCAS",
    ]
    REFINITIV_TEMPLATES = [
        "IncomeStatement"
    ]

    for tmpl in REFINITIV_TEMPLATES:

        org_id = f"TR.F.{tmpl}.orgID"  # OrganizationID
        datadate = f"TR.F.{tmpl}.periodenddate"  # Period End Date
        prelim = f"TR.F.{tmpl}.StmtPrelimFlag"  # Is Preliminary (True/False)
        consol = f"TR.F.{tmpl}.FundConsol"  # Consolidation Basis
        curr = f"TR.F.{tmpl}.currency"  # Currency
        actgstd = f"TR.F.{tmpl}.FundamentalAcctStd"  # Accounting Standard
        seg_name = f"TR.F.{tmpl}.segmentName"  # Segment Name, only if segment data is downloaded
        var_name = (
            f"TR.F.{tmpl}.FccItemName"  # FCC Item Name, a.k.a. variable name
        )
        var_value = f"TR.F.{tmpl}.value"  # FCC Item Name Value, a.k.a the actual data sought
        print(" ")
        print(f"Running on template {tmpl}:")

        for yr in range(FIRST_YEAR, LAST_YEAR + 1):
            sdate = f"{yr + 1}-12-31"
            period = f"FY{yr}"
            # OUT FILE MGMT
            # File per year
            O_FNAME = f"{tmpl}_{str(yr)}.csv"
            OUT_FNAME_CPL = os.path.join(OUT_PATH, O_FNAME)

            # Remove output file, if it exists
            if os.path.exists(OUT_FNAME_CPL):
                os.remove(OUT_FNAME_CPL)

            # Header to output file?
            with open(OUT_FNAME_CPL, "w", encoding="UTF8", newline="") as f:
                writer = csv.DictWriter(f, delimiter="\t", fieldnames=header)
                writer.writeheader()

            period = f"FY{yr}"  # E.g. "FY2020".

            print(f"SDate :{str(sdate)}. Period: {period}. Template: {tmpl}.")
            # Set the parameters for TR.Field as a dictionary

            own_dict = {
                "Period": period,
                "SDate": sdate,
                # "Scale": scale,
                "ReportType": rep_type,
                "ReportingState": rep_state,
                "ConsolBasis": consol_basis,
                "Curn": curn,
                "AlignType": align,
                "RollPeriods": roll,
                "IncludeSpl": special,
            }

            own_fields = [
                ek.TR_Field(org_id, own_dict),
                ek.TR_Field(datadate, own_dict),
                ek.TR_Field(prelim, own_dict),
                ek.TR_Field(consol, own_dict),
                ek.TR_Field(curr, own_dict),
                ek.TR_Field(actgstd, own_dict),
                ek.TR_Field(var_name, own_dict),
                ek.TR_Field(var_value, own_dict),
                #     ek.TR_Field(seg_name, own_dict),  # Only if segment data is requested
                #     ek.TR_Field("TR.F.IncomeStatement.fperiod", own_dict2),
                #     ek.TR_Field("TR.F.IncomeStatement.rfperiod", own_dict2),
                #     ek.TR_Field("TR.F.IncomeStatement.StdCatName", own_dict2),
                #     ek.TR_Field("TR.F.IncomeStatement.FSIdConsolBasis", own_dict2),
                #     ek.TR_Field("TR.F.IncomeStatement.FCCName", own_dict2),
                #     ek.TR_Field("TR.F.IncomeStatement.FCC"),
                #     ek.TR_Field("TR.F.IncomeStatement.FCCNameShort", own_dict),
            ]
            # The actual retrieval loop
            # I run this in sections to avoid other types of errors such as 'timeout'
            # errors
            for line_start in range(0, len(own_list) + 1, SIZE):
                line_end = line_start + SIZE
                print(f"   + Lines: {str(line_start)}/{str(line_end)}")
                # Have added a retry loop if error since sometimes there are
                # problems in the API-connection
                for rec_attempts in range(10):
                    try:
                        dta, err = ek.get_data(
                            instruments=own_list[line_start:line_end],
                            fields=own_fields
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
                my_idx = list(my_header[1:3])
                dta = dta.dropna(how="any", subset=my_idx)

                # Remove any duplicates
                dta = dta.drop_duplicates()

                # Saves the retrieved Eikon data to out-file, unless empty dta
                if len(dta) > 0:
                    dta = dta.drop("Org ID",  axis="columns")
                    dta = dta.rename(columns={"Instruments": "OrganizationID"})
                    dta.to_csv(
                        OUT_FNAME_CPL,
                        mode="a",
                        sep="\t",
                        encoding="utf-8",
                        index=False,
                        header=False,
                    )
                time.sleep(1)
print("DONE")
