import pickle
import numpy as np
from osgeo import gdal
from water_balance import do_wb
import pandas as pd
import matplotlib.pyplot as plt

nodata = -9999

aws = gdal.Open('ucrb_aws.tif').ReadAsArray()

et_arr = pickle.load(open('data/et_interp.p', 'rb'))
pr_arr = pickle.load(open('data/pr_arr.p', 'rb'))

dr_arr = np.ones_like(et_arr)*nodata
dp_arr = np.ones_like(et_arr)*nodata
phantom_arr = np.ones_like(et_arr)*nodata

for i in range(et_arr.shape[0]):
    for j in range(et_arr.shape[1]):
        if aws[i, j] == nodata:
            continue

        dr_ts, dp_ts, phantom_ts = do_wb(aws[i, j], pr_arr[i, j], et_arr[i, j])

        dr_arr[i, j] = dr_ts
        dp_arr[i, j] = dp_ts
        phantom_arr[i, j] = phantom_ts

pickle.dump(dr_arr, open('data/dr_arr.p', 'wb'))
pickle.dump(dp_arr, open('data/dp_arr.p', 'wb'))

#dts = pd.date_range('2019-01-01', '2020-12-31')
## water year indices
#wyr_inds = (dts>='2019-10-01')&(dts<'2020-10-01')
#
#pr_sum = pr_arr[:, :, wyr_inds].sum(axis=-1)
#dp_sum = dp_arr[:, :, wyr_inds].sum(axis=-1)
#
#pr_min_dp = pr_sum-dp_sum
#eff_prec = np.ma.masked_where(aws==nodata, pr_min_dp)
#
#plt.imshow(eff_prec, cmap='plasma')
#plt.colorbar(label='Eff precip (mm)')
#plt.show()
