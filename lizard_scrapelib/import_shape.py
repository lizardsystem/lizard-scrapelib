import os
from osgeo import ogr
from osgeo import osr
import re
import zipfile

import lizard_connector

# station_locations = {"name": [587471.5011, 1109089.8358]
# station_names = {k: station_locations[k][0] for k in stations.keys()}


def convert_to_wkt(station_locations, EPSG=4326):
    """
    Convert locations to WGS84 (the projection used in Lizard).

    Args:
        station_locations (dict): with station locations and codes.
        EPSG (int): EPSG-projection of the locations.
    Returns:
        A dictionary with stations and their coordinates in WGS84.
    """
    # set reference system
    target = osr.SpatialReference()
    target.ImportFromEPSG(4326)
    if EPSG != 4326:
        source = osr.SpatialReference()
        source.ImportFromEPSG(32648)
        transform = osr.CoordinateTransformation(source, target)

    stations_wgs84 = {}

    for station, c in station_locations.items():
        lon, lat = c
        wkt = lizard_connector.queries.wkt_point(lon, lat)
        if EPSG != 4326:
            point = ogr.CreateGeometryFromWkt(wkt)
            point.Transform(transform)
            wkt = point.ExportToWkt()
        stations_wgs84[station] = {"WKT": wkt, "lon": lon, "lat": lat}
        print('Transformed from:', wkt, 'to:', stations_wgs84[station])

    return stations_wgs84


def create_str_field(layer, name, width=24):
    """
    Creates ogr string field
        layer:
        name:
    :param width:
    :return:
    """
    field_name = ogr.FieldDefn(name, ogr.OFTString)
    field_name.SetWidth(width)
    layer.CreateField(field_name)
    return layer


def create_measuringstation_import_zip(
        station_locations, file_path='asset_import_file', asset_name=None,
        station_type=None, prefix="", frequency="", category="", ESPG=4326):
    if asset_name is None:
        raise TypeError("""Required argument asset_name not given. Please choose one below:
        - MeasuringStation
        - GroundwaterStation""")  # TODO further fill in assets
    if station_type is None:
        raise TypeError("""Required argument station_type not given. Please choose one below:
        - WEATHER = 1
        - SEWERAGE = 2
        - SURFACE_WATER = 3
        - OFFSHORE = 4
        - CATCHMENT = 5

        Dike Monitoring and Conditioning
        - DMC = 6
        - SEISMOMETER = 7
        - RADAR = 8
        - INFRARED = 9
        - INCLINOMETER = 10""")

    filebase = os.path.join(file_path, file_path.replace('.shp', '')
                            .replace('.zip', '').replace('.ini', ''))

    if os.path.exists(file_path):
        for extension in ('.shp', '.dbf', '.prj', '.shx', '.ini', '.zip'):
            try:
                os.remove(filebase + extension)
            except FileNotFoundError:
                print(filebase + extension + " not found.")

    stations_wgs84 = convert_to_wkt(station_locations, EPSG=4326)
    target = osr.SpatialReference()
    target.ImportFromEPSG(4326)

    driver = ogr.GetDriverByName('ESRI Shapefile')
    shapeData = driver.CreateDataSource(file_path)
    print(shapeData, file_path, target, ogr.wkbPoint)
    layer = shapeData.CreateLayer(os.path.basename(file_path), target, ogr.wkbPoint)
    field_code = ogr.FieldDefn("code", ogr.OFTString)
    field_code.SetWidth(24)
    layer.CreateField(field_code)
    field_name = ogr.FieldDefn("name", ogr.OFTString)
    field_name.SetWidth(24)
    layer.CreateField(field_name)

    layerDefinition = layer.GetLayerDefn()
    for station, geometry in stations_wgs84.items():
        wkt = geometry["WKT"]
        if station in station_locations.keys():
            point = ogr.CreateGeometryFromWkt(wkt)
            feature = ogr.Feature(layerDefinition)
            feature.SetGeometry(point)
            feature.SetField("name", (prefix + " " if prefix else ""
                             + station).replace('_', ' '))
            feature.SetField("code", (prefix + "_" if prefix else "" +
                             station).replace(' ', '_'))
            layer.CreateFeature(feature)
    shapeData.Destroy()

    with open(filebase + ".ini", 'w') as ini_file:
        ini_file.write('[general]\n')
        ini_file.write('asset_name = ' + asset_name + '\n')
        ini_file.write('\n')
        ini_file.write('[columns]\n')
        ini_file.write('code = code\n')
        ini_file.write('name = name\n')
        ini_file.write('\n')
        ini_file.write('[defaults]\n')
        ini_file.write('station_type = ' + str(station_type) + '\n')
        if frequency:
            ini_file.write('frequency = ' + str(frequency) + '\n')
        if category:
            ini_file.write('category = ' + str(category) + '\n')

    with zipfile.ZipFile(filebase + '.zip', 'w') as zipped_asset:
        for extension in ('.shp', '.dbf', '.prj', '.shx', '.ini'):
            zipped_asset.write(filebase + extension)

    for extension in ('.shp', '.dbf', '.prj', '.shx', '.ini'):
        os.remove(filebase + extension)


# create_measuringstation_import_zip(asset_name="MeasuringStation", station_type=3, prefix="G4AW_MEKONG")
import datetime
datetime.timedelta(16385)
