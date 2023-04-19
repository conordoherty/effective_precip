from osgeo import gdal
import matplotlib.pyplot as plt
import numpy as np
from pyproj import Transformer

nodata = -9999

aws_ras = gdal.Open('ucrb_aws.tif')
aws_gt = aws_ras.GetGeoTransform()
aws = aws_ras.ReadAsArray()
aws = np.ma.masked_where(aws==nodata, aws)

to_wgs = Transformer.from_crs(aws_ras.GetProjectionRef(), 'epsg:4326')
ul = aws_gt[0], aws_gt[3]
br = aws_gt[0]+aws_gt[1]*aws_ras.RasterXSize, aws_gt[3]+aws_gt[5]*aws_ras.RasterYSize

ul_wgs = to_wgs.transform(*ul)
br_wgs = to_wgs.transform(*br)

ext = [ul_wgs[1], br_wgs[1], br_wgs[0], ul_wgs[0]]

plt.figure(figsize=(8, 4))

#plt.imshow(aws, cmap='magma', extent=ext)
yr = 2019
ras = 'phantom'
for yr in range(2019, 2022):
    #epr_frac = gdal.Open(f'epr_rasters/epr_frac_{yr}.tif').ReadAsArray()
    #epr_frac = np.ma.masked_where(aws.mask, epr_frac)
    #plt.imshow(epr_frac, extent=ext, cmap='RdYlBu', vmin=0, vmax=1)
    
    #pr = gdal.Open(f'epr_rasters/pr_{yr}.tif').ReadAsArray()
    #plt.imshow(pr, cmap='Blues', vmin=133, vmax=350)
    
    #epr = gdal.Open(f'epr_rasters/epr_{yr}.tif').ReadAsArray()
    #epr = np.ma.masked_where(aws.mask, epr)
    #print(epr.min(), epr.max())
    #plt.imshow(epr, cmap='Greens', vmin=16, vmax=350)

    phantom = gdal.Open(f'epr_rasters/phantom_{yr}.tif').ReadAsArray()
    #phantom = np.ma.masked_where(aws.mask, phantom)
    #print(phantom.min(), phantom.max())
    #plt.imshow(phantom, extent=ext, vmin=0, vmax=1335, cmap='nipy_spectral')

    total = gdal.Open(f'epr_rasters/total_et_{yr}.tif').ReadAsArray()
    ph_div_tot = phantom / total
    ph_div_tot = np.ma.masked_where(aws.mask, ph_div_tot)
    plt.imshow(ph_div_tot, vmin=0, vmax=1, extent=ext, cmap='RdYlBu')

    plt.tight_layout()
    #plt.colorbar(label='AWS (mm)')
    #plt.colorbar(label='Eff Pr / Pr')
    #plt.colorbar(label='Pr (mm)')
    #plt.colorbar(label='Pr (mm)')
    #plt.colorbar(label='Phantom (mm)')
    plt.colorbar(label='Phantom / Total ET')
    plt.savefig(f'pres/ph_div_tot_{yr}.png')
    
    plt.clf()
