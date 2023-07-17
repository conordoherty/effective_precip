from osgeo import gdal
import matplotlib.pyplot as plt
import numpy as np
from pyproj import Transformer

nodata = -9999

#aws_ras = gdal.Open('ucrb_aws.tif')
aws_ras = gdal.Open('avg_aws.tif')
aws_gt = aws_ras.GetGeoTransform()
aws = aws_ras.ReadAsArray()
aws = np.ma.masked_where(aws==nodata, aws)

to_wgs = Transformer.from_crs(aws_ras.GetProjectionRef(), 'epsg:4326')
ul = aws_gt[0], aws_gt[3]
br = aws_gt[0]+aws_gt[1]*aws_ras.RasterXSize, aws_gt[3]+aws_gt[5]*aws_ras.RasterYSize

ul_wgs = to_wgs.transform(*ul)
br_wgs = to_wgs.transform(*br)

ext = [ul_wgs[1], br_wgs[1], br_wgs[0], ul_wgs[0]]

in_dir = 'epr_rasters'
#in_dir = 'epr_rasters_avg_aws'
out_dir = 'pres'
#out_dir = 'pres_avg_aws'
suff = ''

plt.figure(figsize=(8, 4))

plt.imshow(aws, cmap='magma', extent=ext)
plt.colorbar(label='AWS (mm)')
plt.tight_layout()
plt.savefig(f'{out_dir}/aws.png')
plt.clf()

ras = 'et_of_aw'
for yr in range(2019, 2022):
    epr_frac = gdal.Open(f'{in_dir}/epr_frac_{yr}.tif').ReadAsArray()
    epr_frac = np.ma.masked_where(aws.mask, epr_frac)
    plt.imshow(epr_frac, extent=ext, cmap='RdYlBu', vmin=0, vmax=1)
    plt.colorbar(label='Eff Pr / Pr')
    plt.title(f'Water year {yr}')
    plt.tight_layout()
    plt.savefig(f'{out_dir}/epr_frac_{yr}{suff}.png')
    plt.clf()
    
    #pr = gdal.Open(f'{in_dir}/pr_{yr}.tif').ReadAsArray()
    #plt.imshow(pr, extent=ext, cmap='Blues', vmin=133, vmax=350)
    #plt.colorbar(label='Pr (mm)')
    #plt.title(f'Water year {yr}')
    #plt.savefig(f'{out_dir}/pr_{yr}.png')
    #plt.clf()
    
    epr = gdal.Open(f'{in_dir}/epr_{yr}.tif').ReadAsArray()
    epr = np.ma.masked_where(aws.mask, epr)
    #print(epr.min(), epr.max())
    plt.imshow(epr, extent=ext, cmap='Greens', vmin=16, vmax=350)
    plt.colorbar(label='Eff Pr (mm)')
    plt.title(f'Water year {yr}')
    plt.savefig(f'{out_dir}/epr_{yr}{suff}.png')
    plt.clf()

    #et_of_aw = gdal.Open(f'{in_dir}/et_of_aw_{yr}.tif').ReadAsArray()
    #et_of_aw = np.ma.masked_where(aws.mask, et_of_aw)
    ##print(et_of_aw.min(), et_of_aw.max())
    #plt.imshow(et_of_aw, extent=ext, vmin=0, vmax=1335, cmap='nipy_spectral')
    #plt.colorbar(label='et_of_aw (mm)')
    #plt.title(f'Water year {yr}')
    #plt.savefig(f'{out_dir}/et_of_aw_{yr}.png')
    #plt.clf()

    #total = gdal.Open(f'{in_dir}/total_et_{yr}.tif').ReadAsArray()
    #et_of_aw_div_tot = et_of_aw / total
    #et_of_aw_div_tot = np.ma.masked_where(aws.mask, et_of_aw_div_tot)
    #plt.imshow(et_of_aw_div_tot, vmin=0, vmax=1, extent=ext, cmap='RdYlBu')
    #plt.colorbar(label='et_of_aw / Total ET')
    #plt.title(f'Water year {yr}')
    #plt.savefig(f'{out_dir}/et_of_aw_div_tot_{yr}.png')
    #plt.clf()
