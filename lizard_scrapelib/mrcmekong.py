import csv
import datetime
import os
import pickle
import re
import urllib.request
import zipfile

from lxml import etree
from osgeo import ogr
from osgeo import osr

import lizard_connector
import lizard_scrapelib.pixml


from secrets import *

missing_dry = ['Thakhek', 'Savanakhet', 'Phnom Penh Port']

stations = {
    "Bassac Chaktomouk": ("ppb", "PPB"),
    "Can Tho": (None, "CAN"),
    "Chau Doc": ("cdo", "CDO"),
    "Chiang Khan": ("ckh", "CKH"),
    "Chiang Saen": ("csa", "CSA"),
    "Jinghong": (None, "JIN"),
    "Khong Chiam": (None, "KHO"),
    "Koh Khel": ('koh', None),
    "Kompong Cham": ('kom', None),
    "Kampong Luong": (None, 'KPL'),
    "Kratie": ('kra', "KRA"),
    "Luang Prabang": ('lua', "LUA"),
    "Manan": (None, "MAN"),
    "Mukdahan": ("muk", "MUK"),
    "Nakhon Phanom": ("nak", "NAK"),
    "Neak Luong": ('nea', None),
    "Nong Khai": ('non', "NON"),
    "Paksane": ("pak", None),
    "Pakse": ("pks", "PKS"),
    "Phnom Penh Port": ("ppp", None),
    "Prek Kdam": ("pre", "PRE"),
    "Savanakhet": ('sav', None),
    "Stung Treng": ('str', "STR"),
    "Tan Chau": ("tch", "TCH"),
    "Thakhek": ("tha", None),
    "Vam Nao": (None, "VAM"),
    "Vien Tiane": ('vie', "VTE")
}


station_locations = {
    "Can Tho": ("Can_Tho", (
        587471.5011,
        1109089.8358)),
    "My Thuan": ("My_Thuan", (
        598129.4583,
        1135817.5275)),
    "Vam Nao": ("Vam_Nao", (
        530263.1072,
        1167075.5036)),
    "Chau Doc": ("Chau_Doc", (
        513888.2811,
        1183805.6832)),
    "Tan Chau": ("Tan_Chau", (
        526714.7502,
        1194300.0793)),
    "Koh Khel": ("Koh_Khel", (
        502741.2455,
        1242825.3858)),
    "Neak Luong": ("Neak_Luong", (
        530458.1346,
        1245062.9039)),
    "O Taroat": ("O_Taroat", (
        436245.375,
        1265794.4738)),
    "Bassac Chaktomouk": ("Bassac_Chaktomouk", (
        493490.8817,
        1277038.5801)),
    "Phnom Penh Port": ("Phnom_Penh_Port", (
        491999.6198,
        1279882.2976)),
    "Oral": ("Oral", (
        405483.6675,
        1289638.189)),
    "Prek Kdam": ("Prek_Kdam", (
        478936.2106,
        1306135.1172)),
    "Trapeang": ("Trapeang", (
        405464.4335,
        1306900.6538)),
    "Kompong Cham": ("Kompong_Cham", (
        542693.9017,
        1314836.7043)),
    "Tuk Phos": ("Tuk_Phos", (
        448688.8528,
        1330449.8103)),
    "Snoul": ("Snoul", (
        664254.9268,
        1335117.4983)),
    "Kratie": ("Kratie", (
        606015.5388,
        1356085.2548)),
    "Kg. Chhnang": ("Kg_Chhnang", (
        467202.5399,
        1355340.7375)),
    "Cham Bac": ("Cham_Bac", (
        592723.2598,
        1357966.8478)),
    "Svay Chra": ("Svay_Chra", (
        645424.1656,
        1353459.8447)),
    "Dap Bat": ("Dap_Bat", (
        364125.5714,
        1365664.2345)),
    "Peam Te": ("Peam_Te", (
        610954.5551,
        1376831.1282)),
    "Kantout": ("Kantout", (
        626605.1953,
        1377325.6177)),
    "Kg. Thmar": ("Kg_Thmar", (
        513812.6363,
        1382106.5353)),
    "Talo": ("Talo", (
        364023.3342,
        1383835.9239)),
    "Pursat": ("Pursat", (
        382721.5516,
        1387131.4889)),
    "Kampong Luong": ("Kampong_Luong", (
        414280.2526,
        1390314.7492)),
    "Buon Me Thuot": ("Buon_Me_Thuot", (
        846234.3337,
        1385083.7699)),
    "Kravanh": ("Kravanh", (
        352475.4365,
        1402111.5668)),
    "Maung Rus": ("Maung_Rus", (
        331752.1129,
        1412555.7162)),
    "Sambor": ("Sambor", (
        604211.8086,
        1410540.0265)),
    "Ban Don": ("Ban_Don", (
        801734.4725,
        1423317.9843)),
    "Pailin": ("Pailin", (
        237972.6185,
        1425260.3649)),
    "Kompong Chen": ("Kompong_Chen", (
        454180.9762,
        1430557.4468)),
    "Buon Ho": ("Buon_Ho", (
        855943.3488,
        1430269.6431)),
    "Okrieng": ("Okrieng", (
        627815.0804,
        1441842.3194)),
    "Koh Gneak": ("Koh_Gneak", (
        720230.2501,
        1442332.7453)),
    "Sondan": ("Sondan", (
        525971.4943,
        1448433.2823)),
    "Battambang": ("Battambang", (
        304714.7381,
        1449097.6139)),
    "Kompong Kdei": ("Kompong_Kdei", (
        428469.8915,
        1451445.7772)),
    "Bovel": ("Bovel", (
        267591.2822,
        1467598.3495)),
    "Seam Bork": ("Seam_Bork", (
        600482.7835,
        1480654.4078)),
    "Tala Bori": ("Tala_Bori", (
        603771.3579,
        1497834.574)),
    "Stung Treng": ("Stung_Treng", (
        603771.3579,
        1497834.574)),
    "Sesan": ("Sesan", (
        616774.2006,
        1498310.7808)),
    "Banteay Srey": ("Banteay_Srey", (
        387661.2404,
        1503311.9199)),
    "Sisophon": ("Sisophon", (
        281563.4657,
        1506063.8065)),
    "O Yadav": ("O_Yadav", (
        754195.4801,
        1505313.3302)),
    "Tbeng Meanchey": ("Tbeng_Meanchey", (
        498307.0405,
        1527819.4631)),
    "Koulen": ("Koulen", (
        469486.8288,
        1528091.6059)),
    "Srey Snam": ("Srey_Snam", (
        340670.0458,
        1526339.6553)),
    "Chong Kal": ("Chong_Kal", (
        347141.1693,
        1540298.0176)),
    "Voeun Sai": ("Voeun_Sai", (
        695731.8669,
        1545067.3416)),
    "Pleiku": ("Pleiku", (
        803219.5741,
        1550718.4603)),
    "Seam Pang": ("Seam_Pang", (
        650006.8071,
        1560869.2194)),
    "Oudor Meanchey": ("Oudor_Meanchey", (
        342216.7629,
        1567416.8343)),
    "Ban Chan Noi": ("Ban_Chan_Noi", (
        594218.0482,
        1583145.9644)),
    "Kontum": ("Kontum", (
        823977.7977,
        1587592.3093)),
    "Dak To": ("Dak_To", (
        805995.1032,
        1621340.7154)),
    "M. May attapeu": ("M_May_attapeu", (
        697985.5478,
        1637871.0541)),
    "Sekong": ("Sekong", (
        698836.8961,
        1668557.5272)),
    "Pakse": ("Pakse", (
        586056.4594,
        1670298.4981)),
    "Moung Nong": ("Moung_Nong", (
        603716.4718,
        1667073.1195)),
    "Nikum 34": ("Nikum_34", (
        665484.6989,
        1678970.551)),
    "Ubon": ("Ubon", (
        484762.2933,
        1683040.5912)),
    "Khong Chiam": ("Khong_Chiam", (
        553422.9538,
        1694326.1205)),
    "Souvanna Khill": ("Souvanna_Khill", (
        588572.2347,
        1702551.8107)),
    "Laongam": ("Laongam", (
        625474.0203,
        1710359.0149)),
    "Khong Sedone": ("Khong_Sedone", (
        586951.1337,
        1722362.7737)),
    "Saravanne": ("Saravanne", (
        653581.7212,
        1737575.0923)),
    "Ban Donghene": ("Ban_Donghene", (
        583605.6062,
        1770078.3628)),
    "Moung Tchepone": ("Moung_Tchepone", (
        627708.5297,
        1773016.7668)),
    "Ban Keng don": ("Ban_Keng_don", (
        533898.8565,
        1789825.6528)),
    "ALuoi": ("ALuoi", (
        740510.6737,
        1794326.086)),
    "Kengkok": ("Kengkok", (
        521672.8581,
        1818308.3155)),
    "Moung Phine": ("Moung_Phine", (
        611264.4639,
        1828296.9369)),
    "Mukdahan": ("Mukdahan", (
        471456.8269,
        1829324.6063)),
    "Savanakhet": ("Savanakhet", (
        471730.2198,
        1830996.664)),
    "Highway bridge": ("Highway_bridge", (
        596986.1046,
        1832937.5171)),
    "Phalan": ("Phalan", (
        629700.2937,
        1846508.6416)),
    "Thakhek": ("Thakhek", (
        478736.5848,
        1923015.9881)),
    "Nakhon Phanom": ("Nakhon_Phanom", (
        478216.8044,
        1923927.1147)),
    "Mahaxai": ("Mahaxai", (
        521031.7858,
        1925456.3235)),
    "Kuanpho": ("Kuanpho", (
        545330.6296,
        1934070.0877)),
    "Ban Signo": ("Ban_Signo", (
        504811.2698,
        1973318.6337)),
    "Ban Tha Kok Dae": ("Ban_Tha_Kok_Dae", (
        370209.4934,
        1975561.0806)),
    "Nong Khai": ("Nong_Khai", (
        258006.1757,
        1978550.5477)),
    "Chiang Khan": ("Chiang_Khan", (
        146524.0117,
        1982332.4427)),
    "Vien Tiane": ("Vien_Tiane", (
        247217.0748,
        1984104.1714)),
    "Paklay": ("Paklay", (
        119965.8209,
        2017012.9588)),
    "Ban Phonsi": ("Ban_Phonsi", (
        404717.9651,
        2024054.6788)),
    "Paksane": ("Paksane", (
        358616.7228,
        2031827.0696)),
    "Ban Pak Kanhoun": ("Ban_Pak_Kanhoun", (
        240224.365,
        2038301.3988)),
    "Moung Mai": ("Moung_Mai", (
        358238.3426,
        2046689.0385)),
    "Pakkanhoung": ("Pakkanhoung", (
        237462.518,
        2050995.803)),
    "Muong Borikhane": ("Muong_Borikhane", (
        366210.8215,
        2052818.838)),
    "Vangvieng": ("Vangvieng", (
        230704.7251,
        2095577.7534)),
    "Sayaboury": ("Sayaboury", (
        117465.2055,
        2130855.9204)),
    "Ban Phiengluang": ("Ban_Phiengluang", (
        295218.3935,
        2158780.6276)),
    "Thoeng": ("Thoeng", (
        -4738.0277,
        2184097.9779)),
    "Xieng Ngeun": ("Xieng_Ngeun", (
        209567.8942,
        2185823.0274)),
    "Ban Mixay Ban": ("Ban_Mixay_Ban", (
        205073.3106,
        2190319.5624)),
    "Pak Beng": ("Pak_Beng", (
        89672.269,
        2201341.9727)),
    "Luang Prabang": ("Luang_Prabang", (
        199230.0939,
        2202080.4316)),
    "Chiang Rai": ("Chiang_Rai", (
        -39473.0168,
        2211319.133)),
    "Chiang Khong": ("Chiang_Khong", (
        19726.7233,
        2248078.4181)),
    "Chiang Saen": ("Chiang_Saen", (
        -13016.3566,
        2249538.8429)),
    "Oudomxay": ("Oudomxay", (
        185696.514,
        2289981.2507)),
    "Muong Ngoy": ("Muong_Ngoy", (
        259051.3429,
        2290796.4635)),
    "Xieng Kok": ("Xieng_Kok", (
        46246.0523,
        2316799.3534)),
    "Muong Namtha": ("Muong_Namtha", (
        127238.2979,
        2318281.9328)),
    "Dien Bien": ("Dien_Bien", (
        293236.1529,
        2363994.9834)),
    "Phongsaly": ("Phongsaly", (
        210505.3117,
        2406007.8032)),
    "Manan": ("Manan", (
        112976.76,
        2427672.6449)),
    "Jinghong": ("Jinghong", (62660.292, 2475087.1434))
}

station_names = {k: station_locations[k][0] for k in stations.keys()}

source = osr.SpatialReference()
source.ImportFromEPSG(32648)
target = osr.SpatialReference()
target.ImportFromEPSG(4326)
transform = osr.CoordinateTransformation(source, target)


stations_wgs84 = {}

for station, (_, c) in station_locations.items():
    lon, lat = c
    wkt = lizard_connector.queries.point(lon, lat)
    point = ogr.CreateGeometryFromWkt(wkt)
    point.Transform(transform)
    new_wkt = point.ExportToWkt()
    lon, lat = re.findall("[\d\.]+", new_wkt)
    stations_wgs84[station] = {"WKT": new_wkt, "lon": lon, "lat": lat}
    print('Transformed from:', wkt, 'to:', stations_wgs84[station])

def create_str_field(layer, name, width=24):
    field_name = ogr.FieldDefn(name, ogr.OFTString)
    field_name.SetWidth(width)
    layer.CreateField(field_name)
    return layer


def create_measuringstation_import_zip(file_path='asset_import_file', asset_name=None, station_type=None, prefix=""):
    if asset_name is None:
        raise TypeError("""Required argument asset_name not given. Please choose one below:
        - MeasuringStation
        - GroundwaterStation""") # TODO further fill in assets
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

    filebase = file_path.replace('.shp', '').replace('.zip', '')\
        .replace('.ini', '')

    if os.path.exists(file_path):
        for extension in ('.shp', '.dbf', '.prj', '.shx', '.ini', '.zip'):
            try:
                os.remove(filebase + extension)
            except FileNotFoundError:
                print(filebase + extension + " not found.")
    driver = ogr.GetDriverByName('ESRI Shapefile')
    shapeData = driver.CreateDataSource(file_path + ".shp")
    layer = shapeData.CreateLayer('layer1', target, ogr.wkbPoint)
    field_code = ogr.FieldDefn("code", ogr.OFTString)
    field_code.SetWidth(24)
    layer.CreateField(field_code)
    field_name = ogr.FieldDefn("name", ogr.OFTString)
    field_name.SetWidth(24)
    layer.CreateField(field_name)
    field_station_type = ogr.FieldDefn("st_type", ogr.OFTInteger)
    field_station_type.SetWidth(6)
    layer.CreateField(field_station_type)

    layerDefinition = layer.GetLayerDefn()
    for station, geometry in stations_wgs84.items():
        wkt = geometry["WKT"]
        if station in station_names.keys():
            point = ogr.CreateGeometryFromWkt(wkt)
            feature = ogr.Feature(layerDefinition)
            feature.SetGeometry(point)
            feature.SetField("name", prefix + " " + station)
            feature.SetField("code", prefix + "_" + station_names[station])
            feature.SetField("st_type", station_type)
            layer.CreateFeature(feature)
    shapeData.Destroy()

    with open(file_path + ".ini", 'w') as ini_file:
        ini_file.write('[general]\n')
        ini_file.write('asset_name = ' + asset_name + '\n')
        ini_file.write('\n')
        ini_file.write('[columns]\n')
        ini_file.write('code = code\n')
        ini_file.write('name = name\n')
        ini_file.write('station_type = st_type\n')
        # ini_file.write('\n')
        # ini_file.write('[defaults]\n')
        # ini_file.write('station_type = ' + str(station_type) + '\n')

    with zipfile.ZipFile(filebase + '.zip', 'w') as zipped_asset:
        for extension in ('.shp', '.dbf', '.prj', '.shx', '.ini'):
            zipped_asset.write(filebase + extension)

    for extension in ('.shp', '.dbf', '.prj', '.shx', '.ini'):
        os.remove(filebase + extension)


create_measuringstation_import_zip(asset_name="MeasuringStation", station_type=3, prefix="G4AW_MEKONG")

def download(url):
    request_obj = urllib.request.Request(url)
    with urllib.request.urlopen(request_obj) as resp:
        encoding = resp.headers.get_content_charset()
        encoding = encoding if encoding else 'UTF-8'
        content = resp.read().decode(encoding)
        return content


def make_csvwriter(filename):
    """Write significant changes to csv."""
    csvfile = open(filename, 'w')
    csvwriter = csv.writer(csvfile, delimiter=';', quotechar='"',
                           quoting=csv.QUOTE_MINIMAL)
    return csvwriter


def walk_element_text(element):
    text = element.text.strip(' \r\n\t') if element.text else ''
    for el in element.getchildren():
        text += walk_element_text(el)
    return text

day = datetime.timedelta(days=1)

def days_in_month(date):
    try:
        incremented_date = datetime.datetime(date.year, date.month + 1, 1)
    except ValueError:
        incremented_date = datetime.datetime(date.year + 1, 1, 1)
    return (incremented_date - day).day


def read_cols(tree, xpath_base, table, row_range, col_range, start_date, null=-999.0):
    result = []
    day = datetime.timedelta(days=1)
    for col in range(*col_range):
        for row in range(*row_range):
            if row - row_range[0] >= days_in_month(start_date - day):
                continue
            xpath = xpath_base.format(row=row, col=col, table=table)
            result = tree.xpath(xpath)
            cell_content = walk_element_text(result[0]) if result else ""
            try:
                yield {
                    "datetime": start_date,
                    "value": float(cell_content),
                    "flag": 0
                }
            except ValueError:
                yield {
                    "datetime": start_date,
                    "value": null,
                    "flag": 0
                }
            start_date += day


def create_timeseries_pixml():
    precipitation_headerdicts = {}
    waterlevel_headerdicts = {}
    precipitation_values = {}
    waterlevel_values = {}
    for station_name, station_code in station_names.items():
        if stations[station_name][0] is None:
            continue
        data_precipitation = []
        data_waterlevel = []
        code = 'G4AW_MEKONG_' + station_code
        name_ = 'G4AW_MEKONG ' + station_name
        geometry = stations_wgs84[station_name]
        # waterlevels_name = 'G4AW_MEKONG_waterlevels_' + station_code
        parameter_referenced_unit_waterlevels = "WNS2186"
        # precipitation_name = 'G4AW_MEKONG_precipitation_' + station_code
        parameter_referenced_unit_precipitation = "WNS1400"
        print('loading station', station_name)
        for year in range(2008, 2016):
            flood_url = waterlevels_flood.format(
                year=year, station_name=stations[station_name][0])
            print('"flood" url:', flood_url)
            flood_html = download(flood_url)
            flood_html = re.sub("<!--[past_vlrin_send]+[0-9]+-->", "", flood_html)

            flood_tree = etree.HTML(flood_html)
            flood_xpath = '//*[@id="table{table}"]/tr[{row}]/td[{col}]/font'
            start_date = datetime.datetime(year=year, month=6, day=1)
            data_waterlevel += list(read_cols(
                tree=flood_tree,
                xpath_base=flood_xpath,
                table=6,
                row_range=(3, 34),
                col_range=(2, 7),
                start_date=start_date
            ))
            data_precipitation += list(read_cols(
                tree=flood_tree,
                xpath_base=flood_xpath,
                table=7,
                row_range=(3, 34),
                col_range=(2, 7),
                start_date=start_date
            ))

        for years in ('2013_2014', '2014_2015', '2015_2016'):
            if station_name in missing_dry:
                continue
            dry_url = waterlevels_dry.format(
                year=years, station_name=stations[station_name][0])
            dry_html = download(dry_url)
            dry_html = re.sub("<!--[past_vlrin_send]+[0-9]+-->", "", dry_html)

            dry_tree = etree.HTML(dry_html)
            dry_xpath = './/*[@id="table{table}"]/tr[{row}]/td[{col}]/font'
            print('"dry" url:', dry_url)
            start_date = datetime.datetime(
                year=int(years.split('_')[0]),
                month=11,
                day=1
            )
            data_waterlevel += list(read_cols(
                tree=dry_tree,
                xpath_base=dry_xpath,
                table=6,
                row_range=(3, 34),
                col_range=(2, 9),
                start_date=start_date
            ))
        precipitation_values[code] = data_precipitation
        waterlevel_values[code] = data_waterlevel
        waterlevel_headerdicts[code] = lizard_scrapelib.pixml.header(
            locationId=code,
            parameterId=parameter_referenced_unit_waterlevels,
            stationName=name_,
            lat=geometry['lat'],
            lon=geometry['lon'],
            units="m"
        )
        precipitation_headerdicts[code] = lizard_scrapelib.pixml.header(
            locationId=code,
            parameterId=parameter_referenced_unit_precipitation,
            stationName=name_,
            lat=geometry['lat'],
            lon=geometry['lon'],
            units="mm"
        )
    lizard_scrapelib.pixml.create(waterlevel_headerdicts, waterlevel_values,
        filename="waterlevel_pixml_for_lizard.xml", timeZone=0.0)
    lizard_scrapelib.pixml.create(precipitation_headerdicts, precipitation_values,
                 filename="precipitation_pixml_for_lizard.xml", timeZone=0.0)


def create_timeseries_api(organisation):
    timeseries_location = lizard_connector.connector._Endpoint(
        base="http://integration.nxt.lizard.net",
        endpoint="locations",
        username=USR,
        password=PWD
    )
    timeseries_waterlevel = lizard_connector.connector._Endpoint(
        base="http://integration.nxt.lizard.net",
        endpoint="timeseries",
        username=USR,
        password=PWD
    )
    timeseries_precipitation = lizard_connector.connector._Endpoint(
        base="http://integration.nxt.lizard.net",
        endpoint="timeseries",
        username=USR,
        password=PWD
    )
    timeseries_information = []
    locations_information = []
    for name, station in station_names.items():
        code = 'G4AW_MEKONG_' + station
        name_ = 'G4AW_MEKONG_' + name
        location_data = {
            "name": name_,
            "organisation": organisation,
            "organisation_code": code,
            "geometry": stations_wgs84[name],
            "access_modifier": 100,
        }
        print(location_data)
        location_info = timeseries_location.upload(data=location_data)
        locations_information.push(locations_information)
        timeseries_waterlevel_data = {
            "name": 'G4AW_MEKONG_waterlevels_' + station,
            "location": location_info.get('uuid'),
            "access_modifier": 100,
            "parameter_referenced_unit": "WNS2186",
        }
        timeseries_precipitation_data = {
            "name": 'G4AW_MEKONG_precipitation_' + station,
            "location": location_info.get('uuid'),
            "access_modifier": 100,
            "parameter_referenced_unit": "WNS1400",
        }
        print(timeseries_waterlevel_data)
        print(timeseries_precipitation_data)
        # timeseries_information.append((
        #     timeseries_precipitation.upload(
        #         data=timeseries_precipitation_data),
        #     timeseries_waterlevel.upload(data=timeseries_waterlevel_data)
        # ))
    with open('locations.p', 'wb') as loc_f:
        pickle.dump(locations_information, loc_f)
    with open('timeseries.p', 'wb') as ts_f:
        pickle.dump(timeseries_information, ts_f)
    from pprint import pprint
    pprint(locations_information)
    pprint(timeseries_information)


waterlevels_flood = "http://ffw.mrcmekong.org/historical_data/{year}" \
                    "/stations_historical/historical_{station_name}.htm"
waterlevels_dry = "http://ffw.mrcmekong.org/historical_data_dry/" \
                  "{year}/stations_dry/historical_dry_{station_name}.htm"


def load_historical_mekong_data():
    timeseries = lizard_connector.connector._Endpoint(
        base="http://integration.nxt.lizard.net",
        endpoint="timeseries",
        username=USR,
        password=PWD
    )

    timeseries_results = []

    with open('timeseries.p', 'rb') as ts_f:
        timeseries_information = pickle.load(ts_f)

    for timeseries_precipitation_info, timeseries_waterlevel_info in \
            timeseries_information:
        station_name = timeseries_precipitation_info.get(
            'name'.replace('G4AW_MEKONG_precipitation_', ''))
        print('loading station', station_name)
        for year in range(2008, 2016):
            flood_url = waterlevels_flood.format(
                year=year, station_name=station_name)
            print('"flood" url:', flood_url)
            flood_html = download(flood_url)
            flood_html = re.sub("<!--[past_vlrin_send]+[0-9]+-->", "", flood_html)

            flood_tree = etree.HTML(flood_html)
            flood_xpath = '//*[@id="table{table}"]/tr[{row}]/td[{col}]/font'
            data_waterlevel = list(read_cols(
                tree=flood_tree,
                xpath_base=flood_xpath,
                table=6,
                row_range=(3, 34),
                col_range=(2, 7)
            ))
            data_precipitation = list(read_cols(
                tree=flood_tree,
                xpath_base=flood_xpath,
                table=7,
                row_range=(3, 34),
                col_range=(2, 7)
            ))

            uuid_precipitation = timeseries_precipitation_info.get('uuid')
            uuid_waterlevel = timeseries_waterlevel_info.get('uuid')
            print(data_precipitation, data_waterlevel, uuid_precipitation, uuid_waterlevel)

            # timeseries_results.append(timeseries.upload(
            #     data=data_precipitation, uuid=uuid_precipitation))
            # timeseries_results.append(timeseries.upload(
            #     data=data_waterlevel, uuid=uuid_waterlevel))

        for years in ('2013_2014', '2014_2015', '2015_2016'):
            dry_url = waterlevels_dry.format(
                year=years, station_name=station_name)
            dry_html = download(dry_url)
            dry_html = re.sub("<!--[past_vlrin_send]+[0-9]+-->", "", dry_html)

            dry_tree = etree.HTML(dry_html)
            dry_xpath = './/*[@id="table{table}"]/tr[{row}]/td[{col}]/font'
            print('"dry" url:', dry_url)

            data_waterlevel = list(read_cols(
                tree=dry_tree,
                xpath_base=dry_xpath,
                table=6,
                row_range=(3, 34),
                col_range=(2, 9)
            ))
            # timeseries_results.append(timeseries.upload(
            #     data=data_waterlevel, uuid=uuid_waterlevel))
            print(data_waterlevel)

    with open('timeseries_results.p', 'wb') as ts_f:
        pickle.dump(timeseries_results, ts_f)


#
# for year in range(2008, 2016):
#     for station_name in stations:
#         print('loading station', station_name)
#         flood_writer = make_csvwriter('mrcmekong_flood_data_' + station_name
#                                       + '_' + str(year) + '.csv')
#         url = waterlevels_flood.format(
#             year=year, station_name=station_name)
#         print('url:', url)
#         html = download(url)
#         html = re.sub("<!--[past_vlrin_send]+[0-9]+-->", "", html)
#
#         tree = etree.HTML(html)
#         new_col = ['day']
#         col_range = (1, 6)
#         row_range = (2, 3)
#         xpath = '//*[@id="table{table}"]/tr[{row}]/td[{col}]/font'
#         flood_writer.writerow([station_name])
#         flood_writer.writerow(['Observed Water Level ' + str(year)])
#         read_cols(tree, xpath, 6, row_range, col_range, flood_writer)
#         read_cols(tree, xpath, 6, (3, 34), (1, 7), flood_writer)
#         flood_writer.writerow()
#         flood_writer.writerow(['Rainfall (mm)'])
#         read_cols(tree, xpath, 7, row_range, col_range, flood_writer)
#         read_cols(tree, xpath, 7, (3, 34), (1, 7), flood_writer)
#
#
# for year in ('2013_2014', '2014_2015', '2015_2016'):
#     for station_name in stations:
#         print('loading station', station_name)
#         dry_writer = make_csvwriter('mrcmekong_dry_data_' + station_name
#                                       + '.csv')
#         url = waterlevels_dry.format(
#             year=year, station_name=station_name)
#         print('url:', url)
#         html = download(url)
#         tree = etree.HTML(html)
#         new_row = ['day']
#         col_range = (1, 8)
#         row_range = (2, 3)
#         xpath = './/*[@id="table{table}"]/tr[{row}]/td[{col}]/font'
#         dry_writer.writerow([station_name])
#         dry_writer.writerow(['Observed Water Level ' + str(year)])
#         read_rows(tree, xpath, 6, row_range, col_range, dry_writer)
#         read_rows(tree, xpath, 6, (3, 34), (1, 8), dry_writer)


if __name__ == "__main__":
    create_timeseries_pixml()
    pass
    # create_timeseries(organisation=G4AW_VIETNAM_ORGANISATION)
    # load_historical_data()
    # load_current_data()
