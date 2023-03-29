import ee
import numpy as np
from ras_utils import write_ras_same

nodata = -1
scale = 0.001

#ee.Authenticate()
ee.Initialize()

ens = ee.ImageCollection('IDAHO_EPSCOR/GRIDMET')
#ens = ee.ImageCollection('projects/openet/ensemble/conus_gridmet/c02')
#ens = ee.ImageCollection('projects/openet/reference_et/gridmet/daily')
ucrb_rect = ee.Geometry.Polygon([[-108.74822929627041,39.12782363904188],
                                 [-108.61673668152432,39.12782363904188],
                                 [-108.61673668152432,39.2143274348927],
                                 [-108.74822929627041,39.2143274348927],
                                 [-108.74822929627041,39.12782363904188]])

ens = ens.filterDate('2019-01-01', '2021-01-01')\
         .filterBounds(ucrb_rect)

id_list = ens.toList(1000).map(lambda x: ee.Image(x).id()).getInfo()
#img = ens.first()
#img = ens.first().select('et_ensemble_mad').multiply(0.001)
#area = img.clip(ucrb_rect)
#url = area.getThumbURL({'min': 0, 'max': 6})

#band_name = 'et_ensemble_mad'
band_name = 'pr'
for img_id in id_list:
    print(img_id)
    #img = ee.Image(ee.Image('projects/openet/reference_et/gridmet/daily/'+img_id))\
    #        .select('eto'))
    #img = ee.Image(ee.Image('projects/openet/ensemble/conus_gridmet/c02/'+img_id))\
    #        .select(band_name).multiply(0.001)
    img = ee.Image(ee.Image('IDAHO_EPSCOR/GRIDMET/'+img_id))\
            .select(band_name)

    img = img.reproject(crs='EPSG:32613',
                        crsTransform=[30, 0, 141885, 0, -30, 4429215])

    img_dict = img.sampleRectangle(ucrb_rect, defaultValue=nodata).getInfo()
    arr = np.array(img_dict['properties'][band_name], dtype='float')
    if (arr!=nodata).sum() == 0:
        continue
    #arr[arr!=nodata] = arr[arr!=nodata]*scale
    #import ipdb
    #ipdb.set_trace()

    write_ras_same(arr, f'precip/{img_id}.tif', 'ucrb_area_temp.tiff', no_data=nodata)
