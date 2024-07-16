from osgeo import gdal
gdal.UseExceptions()

import pandas as pd
import numpy as np
from numba import njit, prange

#with open('data_dir.txt') as f:
#    data_dir = f.readline()
#    data_dir = data_dir.rstrip()
data_dir = "~/Downloads/FY2024_gNATSGO_Tabular_CSV/"

#muagg_df = pd.read_csv(data_dir+"gnatsgo/muaggatt.csv")
muagg_df = pd.read_csv(data_dir+"muaggatt.csv")
muagg_df = muagg_df[["mukey", "hydgrpdcd"]]

@njit
def make_hydgrp_dict(mukey, hydgrp):
    d = dict()
    for i in range(mukey.size):
        if hydgrp[i] == '':
            d[mukey[i]] = 0
        elif hydgrp[i] == "A":
            d[mukey[i]] = 1
        elif hydgrp[i] == "B":
            d[mukey[i]] = 2
        elif hydgrp[i] == "C":
            d[mukey[i]] = 3
        #elif np.string_.find(hydgrp[i], "D") != -1:
        elif "D" in hydgrp[i]:
            d[mukey[i]] = 4
        else:
            print("error row", i)

    return d

hydgrp_dict = make_hydgrp_dict(np.array(muagg_df.mukey),
                           np.array(muagg_df.hydgrpdcd.fillna(''),
                                    dtype=str))

crop_df = pd.read_csv("cdl_rz_cn_temp.csv")
crop_df["cdl_code"] = crop_df["CDL code"].astype("int")

@njit
def make_crop_cat_dict(crop_code, cn_category):
    d = dict()
    for i in range(crop_code.size):
        d[crop_code[i]] = cn_category[i]

    return d

crop_df["cn_category"] = crop_df["CN category"]\
                         .apply(lambda x: -1 if np.isnan(x) else int(x))
crop_cat_dict = make_crop_cat_dict(np.array(crop_df.cdl_code),
                                   np.array(crop_df.cn_category))

cn_df = pd.read_csv("cn_table.csv")

@njit
def make_cn_dict(cn_category, vals):
    d = dict()
    for i in range(cn_category.size):
        d[cn_category[i]] = vals[i, :]

    return d

cn_dict = make_cn_dict(np.array(cn_df.cn_category),
                       np.array(cn_df[["a", "b", "c", "d"]]))

@njit
def calc_cn(cdl_code, mukey, crop_cat_dict, hydgrp_dict, cn_dict):
    if cdl_code == 0 or mukey == 0:
        return 0

    crop_cat = crop_cat_dict[cdl_code]
    if crop_cat == -1:
        return 0

    hydgrp = hydgrp_dict[mukey]
    if np.isnan(hydgrp):
        return 0

    # hydgrp is 1-4, so subtract 1 for python indexing
    return cn_dict[crop_cat][hydgrp-1]


'''

######################################################################

mukey_ras = gdal.Open(data_dir+"gnatsgo/FY2024_gNATSGO_mukey_grid.tif")

for yr in range(2016, 2024):
    cdl_ras = gdal.Open(data_dir+f"cdl_tifs/{yr}_30m_cdls.tif")
    #print(cdl_ras.GetGeoTransform())

mukey_gt = mukey_ras.GetGeoTransform()
cdl_gt = cdl_ras.GetGeoTransform()

# mukey and cdl use same CRS but GTs are offset
# one grid cell in each direction
# (writing this to work more generally)
mukey_xoff = max(0, -int((mukey_gt[0] - cdl_gt[0]) / 30))
mukey_yoff = max(0, int((mukey_gt[3] - cdl_gt[3]) / 30))

cdl_xoff = max(0, int((mukey_gt[0] - cdl_gt[0]) / 30))
cdl_yoff = max(0, -int((mukey_gt[3] - cdl_gt[3]) / 30))

#print("loading mukey")
## mukey raster is bigger in both directions than cdl
#mukey_arr = mukey_ras.ReadAsArray(xoff=mukey_xoff, yoff=mukey_yoff,
#                                  xsize=cdl_ras.RasterXSize-cdl_xoff,
#                                  ysize=cdl_ras.RasterYSize-cdl_yoff)
#
#print("loading cdl")
#cdl_arr = cdl_ras.ReadAsArray(xoff=cdl_xoff, yoff=cdl_yoff,
#                              ysize=cdl_ras.RasterYSize-cdl_yoff)

@njit(parallel=True)
def make_aws_arr(cdl_arr, mukey_arr, rz_dict, aws_dict):
    dims = cdl_arr.shape
    aws = np.zeros(dims, dtype=np.int16)

    for i in range(dims[0]):
        for j in prange(dims[1]):
            aws[i, j] = calc_aws(cdl_arr[i, j], mukey_arr[i, j],
                                 rz_dict, aws_dict)

    return aws
'''
