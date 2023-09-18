import pickle
import numpy as np
from osgeo import gdal
from water_balance_runoff import do_wb
import pandas as pd
import matplotlib.pyplot as plt

nodata = -9999
out_dir = 'data_runoff'
#out_dir = 'data_irrig'
#out_dir = 'data_avg_aws'

aws = gdal.Open('ucrb_aws.tif').ReadAsArray()
#aws = gdal.Open('avg_aws.tif').ReadAsArray()

et_arr = pickle.load(open('data/et_interp.p', 'rb'))
pr_arr = pickle.load(open('data/pr_arr.p', 'rb'))

dr_arr = np.ones_like(et_arr)*nodata
dp_arr = np.ones_like(et_arr)*nodata
et_of_aw_arr = np.ones_like(et_arr)*nodata

dts = pd.date_range('2018-01-01', '2021-12-31')
#irrig_ts = dts.month.isin(list(range(4, 11))).astype('int')
irrig_ts = None

for i in range(et_arr.shape[0]):
    for j in range(et_arr.shape[1]):
        if aws[i, j] == nodata:
            continue

        dr_ts, dp_ts, et_of_aw_ts = do_wb(aws[i, j], pr_arr[i, j],
                                          et_arr[i, j], irrig_ts=irrig_ts)

        dr_arr[i, j] = dr_ts
        dp_arr[i, j] = dp_ts
        et_of_aw_arr[i, j] = et_of_aw_ts

pickle.dump(dr_arr, open(f'{out_dir}/dr_arr.p', 'wb'))
pickle.dump(dp_arr, open(f'{out_dir}/dp_arr.p', 'wb'))
pickle.dump(et_of_aw_arr, open(f'{out_dir}/et_of_aw_arr.p', 'wb'))

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
