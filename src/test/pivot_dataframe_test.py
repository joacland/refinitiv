"""
This is a file used for learning how to shift from wide to long, and
long to wide format of a dataframe.

"""
import pandas as pd
import pathlib as pl
from src.my_functions import own_functions as own

df = pd.DataFrame(
    {
        "orgid": [
            "10",
            "10",
            "10",
            "10",
            "10",
            "10",
            "20",
            "20",
            "20",
            "20",
            "20",
            "20",
        ],
        "datadate": [
            "2020-12-31",
            "2020-12-31",
            "2019-12-31",
            "2019-12-31",
            "2018-12-31",
            "2018-12-31",
            "2020-06-30",
            "2020-06-30",
            "2019-06-30",
            "2019-06-30",
            "2018-06-30",
            "2018-06-30",
        ],
        "variable": [
            "at",
            "ib",
            "at",
            "ib",
            "at",
            "ib",
            "at",
            "ib",
            "at",
            "ib",
            "at",
            "ib",
        ],
        "value": [1000, 100, 900, 90, 800, 80, 2000, 200, 1900, 190, 1800, 180],
    }
)  # DataFrame in long format
print(df)

# Make long format to wide format
df = df.pivot(index=["orgid", "datadate"], columns="variable", values="value")
df.reset_index(inplace=True)
print(df)

# # OUT FILE NAME OF DATA
# PROJ_PATH = pl.Path.home().joinpath("Documents", "research", "refinitiv")
# OUT_DIR = "out"
# OUT_PATH = PROJ_PATH.joinpath(
#     OUT_DIR, "test_wide.csv"
# )  # Out path for outfile
# own.save_to_csv_file(df, OUT_PATH, mode="w", header=True)

# Make wide format to long format
df = pd.melt(df, id_vars=["orgid", "datadate"], value_vars=["at", "ib"])
df.sort_values(by=["orgid", "datadate", "variable"], ascending=False, inplace=True)
print(df)
