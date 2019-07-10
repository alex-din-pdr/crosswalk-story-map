# -*- coding: utf-8 -*-
"""
Data Process:
    1) Declare import and export variables
    2) Configure voucher dataframe
    3) Configure crosswalk dataframe
    4) Merge crosswalk file (left) and voucher data (right)
    5) Remove any record where HCV_PUBLIC is null
    6) Calculate the estimate of HCV by ZIP code ESTIMATE = HCV_PUBLIC * RES_RATIO
    7) Use a group-by w/ sum function to reduce the records to unique ZIP codes
    8) Export final dataset

"""

import pandas as pd
import time
#
start_time = time.time()
#
# data imports and exports
#
vouchers  = r"Q:\Some\Directory\Crosswalk_Story_Map\Data\Housing_Choice_Vouchers_by_Tract.csv"
#
crosswalk = r"Q:\Some\Directory\Crosswalk_Story_Map\Data\TRACT_ZIP_122017.xlsx"
#
out_data  = r"CQ:\Some\Directory\Crosswalk_Story_Map\Data\vouchers_by_zip.xlsx"
#
# configure voucher data because leading zeros are lost in the CSV format
# filter for Ohio
#
vouchers = r"Q:\Some\Directory\Crosswalk_Story_Map\Data\Housing_Choice_Vouchers_by_Tract.csv"
#
df_vouchers = pd.read_csv(vouchers, delimiter = ",")
#
df_vouchers.columns = map(str.upper, df_vouchers.columns)
#
del df_vouchers["TRACT"]
#
df_vouchers["GEOID"] = df_vouchers["GEOID"].astype(str).apply(lambda x: x.zfill(11))
#
df_vouchers["STATE"] = df_vouchers["STATE"].astype(str).apply(lambda x: x.zfill(2))
#
df_vouchers = df_vouchers[df_vouchers["STATE"] == "39"]
#
# configure crosswalk file
#
xlsx = pd.ExcelFile(crosswalk)
#
df_crosswalk = xlsx.parse(xlsx.sheet_names[0])
#
df_crosswalk.columns = map(str.upper, df_crosswalk.columns)
#
df_crosswalk["TRACT"] = df_crosswalk["TRACT"].astype(str).apply(lambda x: x.zfill(11))
#
# merge both dataframes into a single datframe
#
df = pd.merge(df_crosswalk, df_vouchers, how = "left", left_on = "TRACT", right_on = "GEOID")
#
# remove records where HCV_PUBLIC is null, these are records that do not have voucher counts
#
df = df[~df["HCV_PUBLIC"].isna()]
#
# use the RES_RATIO to calculate the estimate per ZIP code
#
df["HCV_ESTIMATE"] = df["HCV_PUBLIC"] * df["RES_RATIO"]
#
# perform a group-by to reduce to the number of records to only unique records with the total estimate for each 
#
grpby = df.groupby(["ZIP"])[["HCV_ESTIMATE"]].sum().reset_index()
#
# export the dataframe to an xlsx file for mapping in your GIS of choice
#
#writer = pd.ExcelWriter(out_data)
#
#grpby.to_excel(writer, index = False)
#
#writer.save()
#
# tell me how many voucher program participants that are in the original dataset and in the final dataset
#
print("Voucher holders in original dataset: {0:,}".format(df_vouchers["HCV_PUBLIC"].sum()))
#
print("\n")
#
print("Number of records before group-by in merged dataframe: {0:,}".format(len(df)))
#
print("\n")
#
print("Voucher holders in final output dataset: {0:,}".format(grpby["HCV_ESTIMATE"].sum()))
#
# tell me that the program is finished and how long it took to complete the program
#
end_time = round(time.time() - start_time, 5)
#
print("Seconds elapsed: {0}".format(end_time))