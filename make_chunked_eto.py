from osgeo import gdal
import numpy as np
import pickle
from numba import njit, prange, set_num_threads
from pyproj import Transformer
import glob

set_num_threads(3)

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

print('transforming cdl pixel locs')
crop_inds = np.where(np.isin(cdl_arr, crops))
# coords in middle of pixel
x_ind_coords = cdl_gt[0] + crop_inds[1]*cdl_gt[1] + cdl_gt[1]/2
y_ind_coords = cdl_gt[3] + crop_inds[0]*cdl_gt[5] + cdl_gt[5]/2

@njit
def make_coords_arr(crop_inds, x_ind_coords, y_ind_coords):
    num_locs = crop_inds[0].size
    arr = np.empty((num_locs, 2))
    for i in range(num_locs):
        arr[i, :] = x_ind_coords[i], y_ind_coords[i]

    return arr

# get coordinates of crop pixels in gridmet projection
crop_coords = make_coords_arr(crop_inds, x_ind_coords, y_ind_coords)
#trans = Transformer.from_crs(cdl_epsg, gridmet_epsg, always_xy=True)
#crop_coords_proj = trans.transform(crop_coords[:, 0], crop_coords[:, 1])

# now cdl in 4326 to match ensemble projection
crop_coords_proj = crop_coords

print('loading eto data')
eto_fns = glob.glob(data_dir+'gridmet_eto/*.tif')
eto_fns.sort()

first_eto = gdal.Open(eto_fns[0])
eto_gt = first_eto.GetGeoTransform()
width, height = first_eto.RasterXSize, first_eto.RasterYSize
first_eto = None

eto_arr = np.zeros((height, width, len(eto_fns)))
for i in range(len(eto_fns)):
    if i % 100 == 0:
        print(i)
    eto_arr[:, :, i] = gdal.Open(eto_fns[i]).ReadAsArray()


print('getting crop inds in gridmet grid')
@njit
def get_inds(x_coords, y_coords, gt):
    x_inds = ((x_coords - gt[0]) // gt[1]).astype('int')
    y_inds = ((y_coords - gt[3]) // gt[5]).astype('int')

    return x_inds, y_inds

crop_gm_inds = get_inds(crop_coords_proj[:, 0], crop_coords_proj[:, 1], eto_gt)
crop_gm_inds_arr = np.vstack(crop_gm_inds).T

# pass chunks of inds to this function
@njit
def get_gridmet_vals(gm_arr, crop_inds):
    num_locs = crop_inds.shape[0]
    num_days = gm_arr.shape[-1]
    out_arr = np.empty((num_locs, num_days), dtype=np.uint16)

    for i in range(num_locs):
        out_arr[i, :] = (gm_arr[crop_inds[i, 0], crop_inds[i, 1], i] * ETO_SCALE).astype('uint16')

    return out_arr

print('writing chunks')
chunk_size = int(1e6)
for i, row in enumerate(range(0, crop_gm_inds_arr.shape[0], chunk_size)):
    print(f'chunk {i}')
    chunk_arr = get_gridmet_vals(eto_arr, crop_gm_inds_arr[row:row+chunk_size, :])
    chunk_id = str(i).zfill(2)
    np.savez_compressed(data_dir+f'eto_chunked/eto_chunk{chunk_id}', eto=chunk_arr)
