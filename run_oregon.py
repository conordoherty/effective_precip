import pickle
import numpy as np
from osgeo import gdal
from numba import njit, prange, set_num_threads, float32
import sys, time
from concurrent.futures import ThreadPoolExecutor

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
PR_SCALE = 10
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
@njit('UniTuple(float32[:, :], 4)(int64, int64[:, :], uint16[:, :], float32[:, :],' + \
      'float32[:, :], float32[:, :], float32)', parallel=True)
def run_chunk(chunk, crop_inds_arr, aws_arr, pr, ens, eto, nodata=nodata_gm):
    dr = np.ones(eto.shape, dtype='float32')*nodata
    dp = np.ones(eto.shape, dtype='float32')*nodata
    et_of_aw = np.ones(eto.shape, dtype='float32')*nodata
    ro = np.ones(eto.shape, dtype='float32')*nodata

    locs = crop_inds_arr[:, chunk*chunksize:(chunk+1)*chunksize].T

    for i in prange(chunksize):
        aws = aws_arr[locs[i, 0], locs[i, 1]]
        
        # don't run if fewer than 10 obs (arbitrary); some have 0 obs
        if np.isnan(aws) or aws == aws_nodata or (ens[i] != 32767).sum() < 10:
            continue

        dr_ts, dp_ts, et_of_aw_ts, ro_ts = do_wb_interp(aws, pr[i], ens[i], eto[i], nodata=nodata)

        dr[i] = dr_ts
        dp[i] = dp_ts
        et_of_aw[i] = et_of_aw_ts
        ro[i] = ro_ts

    return dr, dp, et_of_aw, ro


def write_arr(var_str, chunk_str, arr, nodata=nodata_gm):
    arr[arr != nodata] = arr[arr != nodata] * SCALE
    arr = arr.astype('uint16')
    np.savez_compressed(data_dir+f'oregon_output/{var_str}{chunk_str}', var=arr)

#for chunk in range(1):
for chunk in range(1, 82):
#for chunk in range(82):
    start = time.time()
    sys.stdout.write(f'chunk {chunk} .... loading')
    sys.stdout.flush()
    chunk_str = str(chunk).zfill(2)

    def load_fun(var_str):
        var_arr = np.load(data_dir+f'{var_str}_chunked/{var_str}_chunk{chunk_str}.npz')[var_str][:, :num_days]
        var_arr = var_arr.astype('float32')
        if var_str == 'ens':
            var_arr[var_arr!=nodata_gm] = var_arr[var_arr!=nodata_gm] / SCALE
        elif var_str == 'pr':
            var_arr = var_arr / PR_SCALE
        else:
            var_arr = var_arr / SCALE

        if var_str == 'eto':
            var_arr = np.maximum(var_arr, MIN_ETO)

        return var_str, var_arr

    #eto = np.load(data_dir+f'eto_chunked/eto_chunk{chunk_str}.npz')['eto'][:, :num_days]
    #eto = eto.astype(np.float32) / SCALE
    #eto = np.maximum(eto, MIN_ETO)

    #pr = np.load(data_dir+f'pr_chunked/pr_chunk{chunk_str}.npz')['pr'][:, :num_days]
    ## precip scale is 10
    #pr = pr.astype(np.float32) / 10

    #ens = np.load(data_dir+f'ens_chunked/ens_chunk{chunk_str}.npz')['ens'][:, :num_days]
    #ens = ens.astype(np.float32)
    #ens[ens!=nodata_gm] = ens[ens!=nodata_gm] / SCALE

    in_vars = ['eto', 'pr', 'ens']

    with ThreadPoolExecutor(max_workers=len(in_vars)) as e:
        res = e.map(load_fun, in_vars)

    data = {x[0] : x[1] for x in res}

    sys.stdout.write(f' .... running')
    sys.stdout.flush()

    dr, dp, et_of_aw, ro = run_chunk(chunk, crop_inds_arr, aws_arr, data['pr'], data['ens'], data['eto'], nodata=nodata_gm)

    import ipdb
    ipdb.set_trace()

    sys.stdout.write(f' .... writing')
    sys.stdout.flush()

    out_vars = ['dr', 'dp', 'et_of_aw', 'ro']
    out_arrs = [dr, dp, et_of_aw, ro]

    def chunk_fun(var_str, arr):
        return write_arr(var_str, chunk_str, arr)

    with ThreadPoolExecutor(max_workers=len(out_vars)) as e:
        e.map(chunk_fun, out_vars, out_arrs)

    #dr[dr != nodata_gm] = dr[dr != nodata_gm] * SCALE
    #dr = dr.astype('uint16')
    #np.savez_compressed(data_dir+f'oregon_output/dr{chunk_str}', dr=dr)

    #dp[dp != nodata_gm] = dp[dp != nodata_gm] * SCALE
    #dp = dp.astype('uint16')
    #np.savez_compressed(data_dir+f'oregon_output/dp{chunk_str}', dp=dp)

    #et_of_aw[et_of_aw != nodata_gm] = et_of_aw[et_of_aw != nodata_gm] * SCALE
    #et_of_aw = et_of_aw.astype('uint16')
    #np.savez_compressed(data_dir+f'oregon_output/et_of_aw{chunk_str}', et_of_aw=et_of_aw)

    #ro[ro != nodata_gm] = ro[ro != nodata_gm] * SCALE
    #ro = ro.astype('uint16')
    #np.savez_compressed(data_dir+f'oregon_output/ro{chunk_str}', ro=ro)

    sys.stdout.write(f' .... {np.round(time.time() - start, 1)} seconds \n')
    sys.stdout.flush()
    




#do_wb_interp(taw_full, pr_ts, et_ts, eto_ts, irrig_ts=None):
