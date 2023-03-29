import pandas as pd
from osgeo import gdal
import numpy as np
import glob
import pickle

nodata = -9999

dt_range = pd.date_range('2019-01-01', '2020-12-31')

ras = gdal.Open('bc_eto/20190101.tif')

eto_fns = glob.glob('bc_eto/*.tif')
eto_fns.sort()

eto_arr = np.ones((ras.RasterYSize, ras.RasterXSize, dt_range.size))*nodata
et_arr = np.ones((ras.RasterYSize, ras.RasterXSize, dt_range.size))*nodata
pr_arr = np.ones((ras.RasterYSize, ras.RasterXSize, dt_range.size))*nodata

for i, fn in enumerate(eto_fns):
    #if i % 100 == 0:
    #    print(fn)

    eto_arr[:, :, i] = gdal.Open(fn).ReadAsArray()

    dt = dt_range[i]
    pr_fns = glob.glob(f'precip/*{dt.strftime("%Y%m%d")}.tif')
    pr_arr[:, :, i] = gdal.Open(pr_fns[0]).ReadAsArray()

    et_ens_fns = glob.glob(f'et_ens/*{dt.strftime("%Y%m%d")}.tif')
    if len(et_ens_fns) > 0:
        et_day = gdal.Open(et_ens_fns[0]).ReadAsArray()
        # nodata is -1 for et
        et_day[et_day==-1] = nodata
        et_arr[:, :, i] = et_day
        a = gdal.Open(et_ens_fns[0])
        print(et_ens_fns[0], a.RasterYSize, a.RasterXSize)

pickle.dump(eto_arr, open('eto_arr.p', 'wb'))
pickle.dump(et_arr, open('et_arr.p', 'wb'))
pickle.dump(pr_arr, open('pr_arr.p', 'wb'))
