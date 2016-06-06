import datetime
import re
import urllib.request
from lxml import etree

try:
    from utils import command
    from utils import import_shape
    from utils import lizard
    from utils import pixml
except ImportError:
    from lizard_scrapelib.utils import command
    from lizard_scrapelib.utils import import_shape
    from lizard_scrapelib.utils import lizard
    from lizard_scrapelib.utils import pixml


# Start logger and read configuration
logger = command.setup_logger(__name__)
CONFIG = command.read_config('mekong')
CONFIG['station_names'] = {
    k: CONFIG['station_locations'][k] for k in CONFIG['stations'].keys()}
DAY = datetime.timedelta(days=1)


def download(url):
    request_obj = urllib.request.Request(url)
    with urllib.request.urlopen(request_obj) as resp:
        encoding = resp.headers.get_content_charset()
        encoding = encoding if encoding else 'UTF-8'
        content = resp.read().decode(encoding)
        return content


def walk_element_text(element):
    text = element.text.strip(' \r\n\t') if element.text else ''
    for el in element.getchildren():
        text += walk_element_text(el)
    return text


def days_in_month(date):
    try:
        incremented_date = datetime.datetime(date.year, date.month + 1, 1)
    except ValueError:
        incremented_date = datetime.datetime(date.year + 1, 1, 1)
    return (incremented_date - DAY).day


def read_cols(tree, xpath_base, table, row_range, col_range, start_date, null=-999.0):
    for col in range(*col_range):
        for row in range(*row_range):
            if row - row_range[0] >= days_in_month(start_date - DAY):
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
            start_date += DAY


#
# def create_timeseries_api(organisation):
#     stations_wgs84 = import_shape.convert_to_wkt(CONFIG['station_locations'])
#     timeseries_information = []
#     locations_information = []
#     for name, station in CONFIG['station_names'].items():
#         code = 'G4AW_MEKONG_' + station
#         name_ = 'G4AW_MEKONG_' + name
#         location_data = {
#             "name": name_,
#             "organisation": organisation,
#             "organisation_code": code,
#             "geometry": stations_wgs84[name]["WKT"],
#             "access_modifier": 100,
#             "ddsc_show_on_map": False,
#         }
#         # print(location_data)
#         # location_info = timeseries_location.upload(data=location_data)
#         # locations_information.append(location_info)
#         timeseries_waterlevel_data = {
#             "name": 'G4AW_MEKONG_waterlevels_Mukdahan',# 'G4AW_MEKONG_waterlevels_' + station,
#             "location":  '53500c68-701d-4314-83ec-049a0d6085e8', #location_info.get('uuid'),
#             "organisation_code": "G4AW_MEKONG_waterlevels_Mukdahan",
#             "access_modifier": 100,
#             "supplier_code": "G4AW_waterlevels",
#             "supplier": None,
#             "parameter_referenced_unit": "WNS2186",
#             "value_type": 1,
#         }
#         timeseries_precipitation_data = {
#             "name": 'G4AW_MEKONG_precipitation_Mukdahan',  #'G4AW_MEKONG_precipitation_' + station,
#             "location": '60421cf9-2948-4ce0-b4c7-827c91616cdb', #location_info.get('uuid'),
#             "organisation_code": "G4AW_MEKONG_precipitation_Mukdahan",
#             "access_modifier": 100,
#             "supplier_code": None,
#             "supplier": None,
#             "parameter_referenced_unit": "WNS1400",
#             "value_type": 1,
#             # "device": "",
#             # "threshold_min_soft": None,
#             # "threshold_min_hard": None,
#             # "threshold_max_soft": None,
#             # "threshold_max_hard": None
#         }
#         timeseries_information.append((
#             # timeseries_precipitation.upload(
#             #     data=timeseries_precipitation_data),
#             timeseries_waterlevel.upload(data=timeseries_waterlevel_data)
#         ))
#         break
#     with open('locations.p', 'wb') as loc_f:
#         pickle.dump(locations_information, loc_f)
#     with open('timeseries.p', 'wb') as ts_f:
#         pickle.dump(timeseries_information, ts_f)
#     from pprint import pprint
#     pprint(locations_information)
#     pprint(timeseries_information)
#

def load_historical_data(
        first_year, last_year, codes=CONFIG['codes'], break_on_error=False):

    timeseries_information = lizard.find_timeseries_uuids(CONFIG)

    for station, (station_name, _) in CONFIG['stations'].items():
        if not station_name:
            continue

        logger.info('loading station', station)
        for year in range(first_year, last_year + 1):
            start_date = datetime.datetime(year=year, month=6, day=1)
            flood_url = CONFIG['urls']['historic_flood'].format(
                year=year, station_name=station_name)
            logger.debug('"flood" url:', flood_url)
            flood_html = download(flood_url)
            flood_html = re.sub("<!--[past_vlrin_send]+[0-9]+-->", "",
                                flood_html)
            flood_tree = etree.HTML(flood_html)
            flood_xpath = '//*[@id="table{table}"]/tr[{row}]/td[{col}]/font'
            data_waterlevel = read_cols(
                tree=flood_tree,
                xpath_base=flood_xpath,
                table=6,
                row_range=(3, 34),
                col_range=(2, 7),
                start_date=start_date
            )
            timeseries_data = (
                "G4AW_MEKONG " + station, "WNS2186", data_waterlevel)
            lizard.upload_timeseries_data(CONFIG, timeseries_data,
                                          timeseries_information)

            data_precipitation = list(read_cols(
                tree=flood_tree,
                xpath_base=flood_xpath,
                table=7,
                row_range=(3, 34),
                col_range=(2, 7)
            ))
            timeseries_data = (
                "G4AW_MEKONG " + station, "WNS1400", data_precipitation)
            lizard.upload_timeseries_data(CONFIG, timeseries_data,
                                          timeseries_information)

        for years in (str(year) + "_" + str(year + 1) for year in
                      range(first_year, last_year + 1) if year > 2012):
            dry_url = CONFIG['urls']['historic_dry'].format(
                year=years, station_name=station_name)
            dry_html = download(dry_url)
            dry_html = re.sub("<!--[past_vlrin_send]+[0-9]+-->", "", dry_html)

            dry_tree = etree.HTML(dry_html)
            dry_xpath = './/*[@id="table{table}"]/tr[{row}]/td[{col}]/font'
            logger.debug('"dry" url:', dry_url)
            start_date = datetime.datetime(
                year=int(years.split('_')[0]),
                month=11,
                day=1
            )
            data_waterlevel = list(read_cols(
                tree=dry_tree,
                xpath_base=dry_xpath,
                table=6,
                row_range=(3, 34),
                col_range=(2, 9),
                start_date=start_date
            ))
            timeseries_data = (
                "G4AW_MEKONG " + station, "WNS2186", data_waterlevel)
            lizard.upload_timeseries_data(CONFIG, timeseries_data,
                                          timeseries_information)


def create_timeseries_pixml():
    precipitation_headerdicts = {}
    waterlevel_headerdicts = {}
    precipitation_values = {}
    waterlevel_values = {}
    stations_wgs84 = import_shape.convert_to_wkt(CONFIG['station_locations'])
    for station_name, station_code in CONFIG['station_names'].items():
        if CONFIG['stations'][station_name][0] is None:
            continue
        data_precipitation = []
        data_waterlevel = []
        code = 'G4AW_MEKONG_' + str(station_code)
        name_ = 'G4AW_MEKONG ' + str(station_name)
        geometry = stations_wgs84[station_name]
        # waterlevels_name = 'G4AW_MEKONG_waterlevels_' + station_code
        parameter_referenced_unit_waterlevels = "WNS2186"
        # precipitation_name = 'G4AW_MEKONG_precipitation_' + station_code
        parameter_referenced_unit_precipitation = "WNS1400"
        print('loading station', station_name)
        for year in range(2008, 2016):
            flood_url = CONFIG['urls']['historic_flood'].format(
                year=year, station_name=CONFIG['stations'][station_name][0])
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
            if station_name in CONFIG['missing_dry']:
                continue
            dry_url = CONFIG['urls']['historic_dry'].format(
                year=years, station_name=CONFIG['stations'][station_name][0])
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
        waterlevel_headerdicts[code] = pixml.header(
            locationId=code,
            parameterId=parameter_referenced_unit_waterlevels,
            stationName=name_,
            lat=geometry['lat'],
            lon=geometry['lon'],
            units="m"
        )
        precipitation_headerdicts[code] = pixml.header(
            locationId=code,
            parameterId=parameter_referenced_unit_precipitation,
            stationName=name_,
            lat=geometry['lat'],
            lon=geometry['lon'],
            units="mm"
        )
    pixml.create(waterlevel_headerdicts, waterlevel_values,
        filename="waterlevel_pixml_for_lizard.xml", timeZone=0.0)
    pixml.create(precipitation_headerdicts, precipitation_values,
                 filename="precipitation_pixml_for_lizard.xml", timeZone=0.0)


def main():
    args = command.argparser(CONFIG)
    logger.info('################# START MEKONG #################')

    last_value_days_till_now = (
        datetime.datetime.now() -
        datetime.datetime.strptime(CONFIG['last_value_timestamp'], '%Y-%m-%d')
    ).days

    if args.start_year:
        logger.info('Start year found, grabbing data yearly from %s to %s for '
                    'element types: %s', args.start_year, args.end_year,
                    args.codes)
        load_historical_data(args.start_year,
                             args.end_year,
                             codes=args.codes,
                             break_on_error=False)
        return
    elif last_value_days_till_now < 365:
        logger.info('Collecting data from %s till now',
                    CONFIG['last_value_timestamp'])
        start_year = datetime.datetime.strptime(CONFIG['last_value_timestamp'],
                                                "%Y-%m-%d").year
        load_historical_data(start_year,
                             args.end_year,
                             codes=args.codes,
                             break_on_error=False)
        # TODO: grab_recent(args.codes)
    else:
        logger.info('Nothing done.')


if __name__ == "__main__":
    main()
