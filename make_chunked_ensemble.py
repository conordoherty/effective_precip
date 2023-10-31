from osgeo import gdal
import numpy as np
import pickle
from numba import njit
from pyproj import Transformer
import glob
import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor

gdal.UseExceptions()

with open('data_dir.txt') as f:
    data_dir = f.readline()
    data_dir = data_dir.rstrip()

cdl_fn = data_dir+'oregon/oregon_cdl_4326.tif'
sim_ag_keys_fn = data_dir+'oregon/crops.p'
gm_dir = data_dir+'gridmet/'
nodata_uint = 32767
nodata = -9999.
gridmet_epsg = 4326
cdl_epsg = 5070
gridmet_scale = 0.1
ETO_SCALE = 100

with open(sim_ag_keys_fn, 'rb') as f:
    crops = pickle.load(f)

print('reading cdl data')
cdl_ras = gdal.Open(cdl_fn, gdal.GA_ReadOnly)
cdl_gt = cdl_ras.GetGeoTransform()
cdl_arr = cdl_ras.ReadAsArray()
cdl_height, cdl_width = cdl_arr.shape
cdl_ras = None

ens_fns = glob.glob(data_dir+'ensemble/*.tif')
ens_fns.sort()

print('loading ens data')
first_ens = gdal.Open(ens_fns[0])
ens_gt = first_ens.GetGeoTransform()
width, height = first_ens.RasterXSize, first_ens.RasterYSize
first_ens = None

# get doy indices of overpass days
# extremely janky
def ind_from_dt(yr, mo, day):
    op_dt = datetime.datetime(int(yr), int(mo), int(day))
    return (op_dt - datetime.datetime(int(2016), 1, 1)).days

op_inds = [ind_from_dt(*x[-14:-4].split('_')) for x in ens_fns]

crop_inds = np.where(np.isin(cdl_arr, crops))
crop_inds_arr = np.vstack(crop_inds).T

@njit
def make_out_arr(pix_vals):
    pix_vals[pix_vals != nodata] = pix_vals[pix_vals != nodata] * ETO_SCALE
    pix_vals[pix_vals == nodata] = nodata_uint

    return np.round(pix_vals, decimals=0).astype('uint16')

# pass chunks of inds to this function
def get_ens_vals(crop_inds, num_days):
    num_locs = crop_inds.shape[0]
    out_arr = np.ones((num_locs, num_days), dtype=np.uint16)*nodata_uint

    min_y, min_x = crop_inds.min(axis=0)
    size_y, size_x = crop_inds.max(axis=0) - crop_inds.min(axis=0)

    for i in range(num_days):
        if i in op_inds:
            ras = gdal.Open(ens_fns[op_inds.index(i)], gdal.GA_ReadOnly)
            area = ras.ReadAsArray(int(min_x), int(min_y), int(size_x)+1, int(size_y)+1)
            ras = None
            if (area != nodata).sum() == 0:
                continue

            pix_vals = area[crop_inds[:, 0]-min_y, crop_inds[:, 1]-min_x]
            out_arr[:, i] = make_out_arr(pix_vals)
        else:
            # no et produced for this day
            continue

    return out_arr

chunk_size = int(1e6)
#for i, row in enumerate(range(0, crop_inds_arr.shape[0], chunk_size)):
def make_chunk(chunk_num, chunk_size, crop_inds_arr):
    print(f'starting chunk {chunk_num}')

    start_ind = chunk_num*chunk_size
    chunk_arr = get_ens_vals(crop_inds_arr[start_ind:start_ind+chunk_size, :], 2556)
    chunk_id = str(chunk_num).zfill(2)
    
    #return data_dir+f'ens_chunked/ens_chunk{chunk_id}', chunk_arr
    print(f'writing chunk {chunk_num}')
    np.savez_compressed(data_dir+f'ens_chunked/ens_chunk{chunk_id}', ens=chunk_arr)


if __name__ == '__main__':
    #with ProcessPoolExecutor(max_workers=8, max_tasks_per_child=2) as e:
    with ThreadPoolExecutor(max_workers=8) as e:
        #futures = [e.submit(make_chunk, chunk_num, chunk_size, crop_inds_arr) for chunk_num in range(83)]
        for chunk_num in range(83):
            e.submit(make_chunk, chunk_num, chunk_size, crop_inds_arr)
    
        #for future in as_completed(futures):
        #    print("here")
        #    print(future)
        #    res = future.result()
        #    np.savez_compressed(res[0], ens=res[1])
        #    del future

#for i in range(10):
#    a, b = make_chunk(i, chunk_size, crop_inds_arr)
#    np.savez_compressed(a, ens=b)
