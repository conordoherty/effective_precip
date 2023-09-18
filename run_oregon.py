import pickle
import numpy as np
from osgeo import gdal
from numba import njit, prange, set_num_threads
import sys, time

gdal.UseExceptions()
set_num_threads(24)

from water_balance_runoff import do_wb_interp

with open('data_dir.txt') as f:
    data_dir = f.readline()
    data_dir = data_dir.rstrip()

# 2016-2021
num_days = 365*6+2

# bias corrected ETO contains zeros
MIN_ETO = 0.06

ssurgo_dir = data_dir+"gssurgo_mukey"
SCALE = 100
uint16_nodata = np.array([-1]).astype('uint16')[0]
nodata_gm = 32767
aws_nodata = 65535
nodata = -9999

sim_ag_keys_fn = data_dir+'oregon/crops.p'
with open(sim_ag_keys_fn, 'rb') as f:
    crops = pickle.load(f)

aws_arr = gdal.Open(ssurgo_dir+'/oregon_aws.tif', gdal.GA_ReadOnly).ReadAsArray()
cdl_arr = gdal.Open(data_dir+'oregon/oregon_cdl_4326.tif', gdal.GA_ReadOnly).ReadAsArray()
crop_inds = np.where(np.isin(cdl_arr, crops))
crop_inds_arr = np.vstack(crop_inds)

chunksize = int(1e6)
@njit(parallel=True)
def run_chunk(chunk, crop_inds_arr, aws_arr, pr, ens, eto, nodata=-9999.):
    dr = np.ones(eto.shape, dtype=np.float32)*np.nan
    dp = np.ones(eto.shape, dtype=np.float32)*np.nan
    et_of_aw = np.ones(eto.shape, dtype=np.float32)*np.nan
    et = np.ones(eto.shape, dtype=np.float32)*np.nan

    locs = crop_inds_arr[:, chunk*chunksize:(chunk+1)*chunksize].T

    for i in prange(chunksize):
        aws = aws_arr[locs[i, 0], locs[i, 1]]
        
        # don't run if fewer than 10 obs (arbitrary); some have 0 obs
        if np.isnan(aws) or aws == aws_nodata or (ens[i] != 32767).sum() < 10:
            continue

        dr_ts, dp_ts, et_of_aw_ts, et_ts = do_wb_interp(aws, pr[i], ens[i], eto[i], nodata=nodata_gm)

        dr[i] = dr_ts
        dp[i] = dp_ts
        et_of_aw[i] = et_of_aw_ts
        et[i] = et_ts

    return dr, dp, et_of_aw, et

for chunk in range(83):
    start = time.time()
    sys.stdout.write(f'chunk {chunk} .... loading')
    sys.stdout.flush()
    chunk_str = str(chunk).zfill(2)

    eto = np.load(data_dir+f'eto_chunked/eto_chunk{chunk_str}.npz')['eto'][:, :num_days]
    eto = eto.astype(np.float32) / SCALE
    eto = np.maximum(eto, MIN_ETO)

    pr = np.load(data_dir+f'pr_chunked/pr_chunk{chunk_str}.npz')['pr'][:, :num_days]
    pr = pr.astype(np.float32) / SCALE

    ens = np.load(data_dir+f'ens_chunked/ens_chunk{chunk_str}.npz')['ens'][:, :num_days]
    ens = ens.astype(np.float32)
    ens[ens!=nodata_gm] = ens[ens!=nodata_gm] / SCALE

    sys.stdout.write(f' .... running')
    sys.stdout.flush()
    dr, dp, et_of_aw, et = run_chunk(chunk, crop_inds_arr, aws_arr, pr, ens, eto, nodata=nodata_gm)

    import ipdb
    ipdb.set_trace()

    sys.stdout.write(f' .... writing')
    sys.stdout.flush()
    dr = (dr * SCALE).astype('uint16')
    np.savez_compressed(data_dir+f'oregon_output/dr{chunk_str}', dr=dr)

    dp = (dp * SCALE).astype('uint16')
    np.savez_compressed(data_dir+f'oregon_output/dp{chunk_str}', dp=dp)

    et_of_aw = (et_of_aw * SCALE).astype('uint16')
    np.savez_compressed(data_dir+f'oregon_output/et_of_aw{chunk_str}', et_of_aw=et_of_aw)

    sys.stdout.write(f' .... {np.round(time.time() - start, 1)} seconds \n')
    sys.stdout.flush()
    




#do_wb_interp(taw_full, pr_ts, et_ts, eto_ts, irrig_ts=None):
