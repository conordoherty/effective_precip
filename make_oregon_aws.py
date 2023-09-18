from osgeo import gdal
import numpy as np
import pandas as pd
from numba import njit
from ras_utils import write_ras_same

gdal.UseExceptions()

nodata = -9999.
gssurgo_nodata = 2147483647
uint16_nodata = np.array([-1]).astype('uint16')[0]
data_dir = '/nobackup/cdohert1/gssurgo_mukey'
mukey_fn = data_dir+'/oregon_mukey.tif'

mukey = gdal.Open(mukey_fn).ReadAsArray()
valu = pd.read_csv(data_dir+'/Valu1.csv')

@njit
def make_mukey_dict(mukey, aws):
    d = dict()
    for i in range(mukey.size):
        d[mukey[i]] = aws[i]

    return d


aws_vals = valu.rootznaws.to_numpy()
aws_vals[np.isnan(aws_vals)] = uint16_nodata
aws_vals = aws_vals.astype('uint16')
aws_dict = make_mukey_dict(valu.mukey.to_numpy(), aws_vals)

@njit
def make_aws(mukey, aws_dict):
    aws = np.ones(mukey.shape, dtype='uint16')*uint16_nodata
    for i in range(mukey.shape[0]):
        for j in range(mukey.shape[1]):
            if mukey[i, j] == gssurgo_nodata or np.isnan(aws_dict[mukey[i, j]]):
                continue
            else:
                aws[i, j] = aws_dict[mukey[i, j]]

    return aws

oregon_aws = make_aws(mukey, aws_dict)
aws_ras_5070 = write_ras_same(oregon_aws, '', mukey_fn, ras_format='MEM', no_data=uint16_nodata)


# grid info
cdl_ras = gdal.Open(data_dir+'/../oregon/oregon_cdl_4326.tif', gdal.GA_ReadOnly)
height = cdl_ras.RasterYSize
width = cdl_ras.RasterXSize
gt = cdl_ras.GetGeoTransform()
srs = cdl_ras.GetSpatialRef()
cdl_ras = None

bounds = (gt[0], gt[3]+gt[5]*height, gt[0]+gt[1]*width, gt[3])
aws_ras = gdal.Warp(data_dir+'/oregon_aws.tif', aws_ras_5070,
                    outputBounds=bounds, width=width, height=height,
                    dstSRS=srs, srcNodata=uint16_nodata,
                    dstNodata=uint16_nodata, resampleAlg='nearest')


#np.savez_compressed(data_dir+'/oregon_aws', aws=oregon_aws)
