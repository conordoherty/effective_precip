from shapely.geometry import Polygon
import geopandas as gpd

poly_coords = [[-108.74822929627041,39.12782363904188],
               [-108.61673668152432,39.12782363904188],
               [-108.61673668152432,39.2143274348927],
               [-108.74822929627041,39.2143274348927],
               [-108.74822929627041,39.12782363904188]]

geom = Polygon(poly_coords)
polygon = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[geom])       

polygon.to_file(filename='ucrb_area.shp', driver="ESRI Shapefile")
