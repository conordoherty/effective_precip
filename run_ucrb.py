import pickle
import numpy as np
from osgeo import gdal
from water_balance import do_wb

aws = gdal.Open('ucrb_aws.tif').ReadAsArray()

et_arr = pickle.load(open('et_interp.p', 'rb'))
pr_arr = pickle.load(open('pr_arr.p', 'rb'))

dr_arr = np.zeros_like(et_arr)
dp_arr = np.zeros_like(et_arr)
phantom_arr = np.zeros_like(et_arr)

for i in range(et_arr.shape[0]):
    for j in range(et_arr.shape[1]):
        dr_ts, dp_ts, phantom_ts = do_wb(aws[i, j], pr_arr[i, j], et_arr[i, j])

        dr_arr[i, j] = dr_ts
        dp_arr[i, j] = dp_ts
        phantom_arr[i, j] = phantom_ts
