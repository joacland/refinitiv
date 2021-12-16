"""
This is what could become a template for a script for downloading
Datastream data.
"""
import DatastreamDSWS as dsws

# We can use our Refinitiv's Datastream Web Socket (DSWS) API keys that allows
# us to be identified by Refinitiv's back-end services and enables us to
# request (and fetch) data: Credentials are placed in a text file so that it
# may be used in this code without showing it itself.

(dsws_username, dsws_password) = (
    open("Datastream_username.txt", "r"),
    open("Datastream_password.txt", "r"),
)
ds = dsws.Datastream(
    username=str(dsws_username.read()), password=str(dsws_password.read())
)

# It is best to close the files we opened in order to make sure that we don't
# stop any other services/programs from accessing them if they need to.
dsws_username.close()
dsws_password.close()

# Alternatively one can use the following:
# import getpass
# dsusername = input()
# dspassword = getpass.getpass()
# ds = dsws.Datastream(username = dsusername, password = dspassword)

import warnings  # ' warnings ' is a native Python library allowing us to raise warnings and errors to users.
from datetime import datetime  # This is needed to keep track of when code runs.
import pandas as pd
import numpy as np

from tqdm.notebook import (
    trange,
)  # ' tqdm ' allows loops to show a progress meter. Version 4.48.2 was used in this article.
import ipywidgets as widgets

# # From the ' Reference data.xls ' file, one could run and use the following:
# xl_countries = pd.read_excel("Reference data.xls", sheet_name = "Countries")
# xl_regions = pd.read_excel("Reference data.xls", sheet_name = "Regions")
# xl_index = pd.read_excel("Reference data.xls", sheet_name = "Index Names 3")
# xl_columns = pd.read_excel("Reference data.xls", sheet_name = "Column Names 2")
# # If you don't have the ' Reference data.xls ' file, just use the bellow:
xl_regions = pd.DataFrame(data={'Region': {0: 'EAFE', 1: 'EAFE + Canada', 2: '@:M1WLDXA'}})
xl_countries = pd.DataFrame(data={'Region': {0: 'Argentina', 1: 'Australia', 2:'@:VEMSCIP'}})
print(xl_regions.head(10))
