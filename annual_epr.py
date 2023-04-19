import pickle
import pandas as pd
from osgeo import gdal
from ras_utils import write_ras_same

nodata = -9999
aws = gdal.Open('ucrb_aws.tif').ReadAsArray()

et = pickle.load(open('data/et_interp.p', 'rb'))
pr = pickle.load(open('data/pr_arr.p', 'rb'))
dp = pickle.load(open('data/dp_arr.p', 'rb'))
dr = pickle.load(open('data/dr_arr.p', 'rb'))
ph = pickle.load(open('data/phantom_arr.p', 'rb'))

dt_range = pd.date_range('2018-01-01', '2021-12-31')
water_years = list(range(2019, 2022))

for wyr in water_years:
    start_dr = dr[:, :, dt_range==f'{wyr-1}-09-30'][:, :, 0]
    start_dr[aws==nodata] = nodata
    write_ras_same(start_dr, f'epr_rasters/start_dr_{wyr}.tif', 'ucrb_aws.tif',
                   no_data=nodata)

    yr_ind = (dt_range>=f'{wyr-1}-10-01')&(dt_range<=f'{wyr}-09-30')
    yr_pr = pr[:, :, yr_ind].sum(axis=-1)
    yr_dp = dp[:, :, yr_ind].sum(axis=-1)
    yr_ph = ph[:, :, yr_ind].sum(axis=-1)
    yr_et = et[:, :, yr_ind].sum(axis=-1)

    epr = yr_pr - yr_dp
    epr[aws==nodata] = nodata

    epr_frac = epr/yr_pr
    epr_frac[aws==nodata] = nodata

    write_ras_same(yr_pr, f'epr_rasters/pr_{wyr}.tif', 'ucrb_aws.tif',
                   no_data=nodata)
    write_ras_same(epr, f'epr_rasters/epr_{wyr}.tif', 'ucrb_aws.tif',
                   no_data=nodata)
    write_ras_same(epr_frac, f'epr_rasters/epr_frac_{wyr}.tif', 'ucrb_aws.tif',
                   no_data=nodata)
    write_ras_same(yr_ph, f'epr_rasters/phantom_{wyr}.tif', 'ucrb_aws.tif',
                   no_data=nodata)
    write_ras_same(yr_et, f'epr_rasters/total_et_{wyr}.tif', 'ucrb_aws.tif',
                   no_data=nodata)