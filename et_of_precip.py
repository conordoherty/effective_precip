from osgeo import gdal
import numpy as np
import matplotlib.pyplot as plt
from ras_utils import write_ras_same

nodata = -9999
in_dir = 'epr_rasters'

for yr in range(2019, 2022):
    total = gdal.Open(in_dir+f'/total_et_{yr}.tif').ReadAsArray()

    phantom = gdal.Open(in_dir+f'/phantom_{yr}.tif').ReadAsArray()

    et_of_precip = total - phantom
    et_of_precip[phantom==nodata] = nodata
    write_ras_same(et_of_precip, f'veget_comp/et_of_precip_{yr}.tif', 'ucrb_aws.tif')
