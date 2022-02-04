"""
This is a test.py

"""
import os
import pandas as pd
import json as js
from src.my_functions import own_functions as own

# SET PANDAS CONFIGURATION
pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("max_colwidth", None)

# WHERE IS, AND WHERE TO PUT, DATA?
SOURCE_PATH = "D:\\"  # where is?
OUT_PATH = "D:\\"  # where to?

if __name__ == "__main__":
    source_fname = "test1.json"  # Name of source file
    source_fname_cpl = os.path.join(SOURCE_PATH, source_fname)
    json1 = own.read_json_file(source_fname_cpl)
    json3 = json1.copy()
    json3.pop("data")
    # json1.pop("rowHeadersCount")
    # json1.pop("totalRowsCount")
    # json1.pop("columnHeadersCount")
    # json1.pop("headerOrientation")
    # json1.pop("totalColumnsCount")
    data1 = json1.pop("data")
    json3["data"] = data1

    source_fname = "test2.json"  # Name of source file
    source_fname_cpl = os.path.join(SOURCE_PATH, source_fname)
    json2 = own.read_json_file(source_fname_cpl)
    data2 = json2.pop("data")
    data3 = data1 + data2
    json3["totalRowsCount"] = len(data1 + data2)
    json3["data"] = data1 + data2

    print(list(json1))
    # print(list(json2))
    print(list(json3))
    print(list(data1))
    # tmp = js.dumps(json3)
    out_fname_cpl = os.path.join(SOURCE_PATH, "test.json")
    own.save_to_json(json3, out_fname_cpl, mode="w")
