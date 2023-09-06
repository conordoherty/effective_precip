from osgeo import gdal
import numpy as np
import pickle
from numba import njit, prange, set_num_threads
from pyproj import Transformer
import glob
import datetime

gdal.UseExceptions()
set_num_threads(1)

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
cdl_ras = gdal.Open(cdl_fn)
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
    return (op_dt - datetime.datetime(int(yr), 1, 1)).days

op_inds = [ind_from_dt(*x[-14:-4].split('_')) for x in ens_fns]

crop_inds = np.where(np.isin(cdl_arr, crops))
crop_inds_arr = np.vstack(crop_inds).T

# pass chunks of inds to this function
def get_ens_vals(crop_inds, num_days):
    num_locs = crop_inds.shape[0]
    out_arr = np.empty((num_locs, num_days), dtype=np.uint16)

    min_y, min_x = crop_inds.min(axis=0)
    size_y, size_x = crop_inds.max(axis=0) - crop_inds.min(axis=0)

    for i in range(num_days):
        if i in op_inds:
            ras = gdal.Open(ens_fns[op_inds.index(i)])
            area = ras.ReadAsArray(int(min_x), int(min_y), int(size_x)+1, int(size_y)+1)
            ras = None
            out_arr[:, i] = area[crop_inds[:, 0]-min_y, crop_inds[:, 1]-min_x]
        else:
            # no et produced for this day
            continue

    return out_arr

print('writing chunks')
chunk_size = int(1e6)
for i, row in enumerate(range(0, crop_inds_arr.shape[0], chunk_size)):
    print(f'chunk {i}')

    chunk_arr = get_ens_vals(crop_inds_arr[row:row+chunk_size, :], 366)
    chunk_id = str(i).zfill(2)
    np.savez_compressed(data_dir+f'ens_chunked/ens_chunk{chunk_id}', eto=chunk_arr)
