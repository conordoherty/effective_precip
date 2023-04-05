from osgeo import gdal, gdal_array
import numpy as np

def write_ras_same(array, new_ras_fn, template_fn, ras_format='GTiff', no_data=None):
    template_ras = gdal.Open(template_fn)
    #template_srs = template_ras.GetSpatialRef()
    template_srs = template_ras.GetProjectionRef()
    template_gt = template_ras.GetGeoTransform()
    template_band = template_ras.GetRasterBand(1)

    if no_data == None:
        template_nodata = template_band.GetNoDataValue()
    else:
        template_nodata = no_data

    rows, cols = array.shape
    if array.dtype == np.int64:
        raise Exception('Array type is int64 which gdal does not understand')
    d_type = gdal_array.NumericTypeCodeToGDALTypeCode(array.dtype)

    driver = gdal.GetDriverByName(ras_format)
    out_ras = driver.Create(new_ras_fn, cols, rows, 1, d_type)
    out_ras.SetGeoTransform(template_gt)
    out_band = out_ras.GetRasterBand(1)
    out_band.WriteArray(array)
    out_band.SetNoDataValue(template_nodata)
    #out_ras.SetProjection(template_srs.ExportToWkt())
    out_ras.SetProjection(template_srs)
    out_band.FlushCache()

    template_band = None
    template_ras = None

    if ras_format != 'MEM':
        out_band = None
        out_ras = None
        return 0
    else:
        return out_ras
