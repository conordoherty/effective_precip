from osgeo import gdal
gdal.UseExceptions()

import pandas as pd
import numpy as np
from numba import njit, prange

with open('data_dir.txt') as f:
    data_dir = f.readline()
    data_dir = data_dir.rstrip()

muagg_df = pd.read_csv(data_dir+"gnatsgo/muaggatt.csv")
muagg_df = muagg_df[["mukey", "hydgrpdcd"]]

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
        elif np.strings.find(hydgrp[i], "D") != -1:
            d[mukey[i]] = 4
        else:
            print("error row", i)

    return d

cn_dict = make_hydgrp_dict(np.array(muagg_df.mukey),
                           np.array(muagg_df.hydgrpdcd.fillna(''),
                                    dtype=str))

crop_df = pd.read_csv("cdl_rz_cn_temp.csv")
crop_df["cdl_code"] = crop_df["CDL code"].astype("int")

@njit
def make_rz_dict(crop_code, depth):
    d = dict()
    for i in range(crop_code.size):
        d[crop_code[i]] = depth[i]

    return d

rz_dict = make_rz_dict(np.array(crop_df.cdl_code),
                         np.array(crop_df.depth))

@njit
def calc_aws(cdl_code, mukey, rz_dict, aws_dict):
    if cdl_code == 0 or mukey == 0:
        return 0

    rz_depth = rz_dict[cdl_code]
    if np.isnan(rz_depth):
        return 0

    aws25, aws50, aws100, aws150 = aws_dict[mukey]

    if rz_depth < 25:
        aws = aws25 * rz_depth / 25
    elif rz_depth >= 25 and rz_depth < 50:
        marginal_frac = (rz_depth - 25) / 25
        marginal_aws = marginal_frac * (aws50 - aws25)
        aws = aws25 + marginal_aws
    elif rz_depth >= 50 and rz_depth < 100:
        marignal_frac = (rz_depth - 50) / 50
        marginal_aws = marginal_frac * (aws100 - aws50)
        aws = aws50 + marginal_aws
    elif rz_depth >= 100 and rz_depth < 150:
        marginal_frac = (rz_depth - 100) / 50
        marginal_aws = marginal_frac * (aws150 - aws100)
        aws = aws100 + marginal_aws
    elif rz_depth >= 150:
        rz_beyond_150 = rz_depth - 150
        deepest_awc = (aws150 - aws100) / 50
        aws = aws150 + rz_beyond_150 * deepest_awc

    # return value in mm
    return aws * 10



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
