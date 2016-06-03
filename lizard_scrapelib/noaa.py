import datetime
import os
import time
import urllib.error

try:
    from pyftpclient import PyFTPclient
    from utils import command
    from utils import ftp
    from utils import import_shape
    from utils import lizard
    from utils import pixml
except ImportError:
    from lizard_scrapelib.pyftpclient import PyFTPclient
    from lizard_scrapelib.utils import command
    from lizard_scrapelib.utils import ftp
    from lizard_scrapelib.utils import import_shape
    from lizard_scrapelib.utils import lizard
    from lizard_scrapelib.utils import pixml

import lizard_connector


# Start logger and read configuration
logger = command.setup_logger(__name__)
CONFIG = command.read_config('noaa')


def parse_file(file_path, data_dir="data", codes=CONFIG['codes'],
               units=CONFIG['units'], break_on_error=False,
               delete_values=False):
    """Uploads a year of data into the Lizard-api for given element types."""
    timeseries = lizard_connector.connector.Endpoint(
        username=CONFIG['login']['username'],
        password=CONFIG['login']['password'],
        base=CONFIG['lizardbase'],
        endpoint='timeseries')
    timeseries_uuids = {}

    for location_name, code, data in read_file(
            codes, file_path, delete_values):
        # get uuid for timeseries and store uuid to dict.
        uuid = timeseries_uuids.get(
            ('NOAA_' + location_name, code),
             lizard.find_timeseries(location_name='NOAA_' + location_name,
                                    config=CONFIG,
                                    code=code,
                                    timeseries_uuids=timeseries_uuids))
        if uuid:
            try:
                logger.info('location %s | code %s | uuid %s has data: %s',
                            location_name, code, uuid, str(data))
                reaction = timeseries.upload(uuid=uuid, data=[data])
                logger.info('location %s | code %s responds after sending '
                            'data: %s',location_name, code, str(reaction))
            except urllib.error.HTTPError as http_error:
                logger.exception(
                    'Error in data found when submitting timeseries '
                    'data. Station: %s, element_type: %s',
                    location_name, code)
                time.sleep(10)
                if break_on_error:
                    raise
    os.remove(file_path)


def read_file(codes, filepath, delete_values=False):
    """
    Reads a year of NOAA data and yields its values.

    The data is stored comma seperated by index:
    [0] ID = 11 character station identification code
    [1] YEAR/MONTH/DAY = 8 character date in YYYYMMDD format
        (e.g. 19860529 = May 29, 1986)
    [2] ELEMENT = 4 character indicator of element type
    [3] DATA VALUE = 5 character data value for ELEMENT
    [4] M-FLAG = 1 character Measurement Flag
    [5] Q-FLAG = 1 character Quality Flag
    [6] S-FLAG = 1 character Source Flag
    [7] OBS-TIME = 4-character time of observation in hour-minute
        format (i.e. 0700 =7:00 am)

    Where Q-FLAG codes refer to:
    [0] Blank = did not fail any quality assurance check
    [1]   D   = failed duplicate check
    [2]   G   = failed gap check
    [3]   I   = failed internal consistency check
    [4]   K   = failed streak/frequent-value check
    [5]   L   = failed check on length of multiday period
    [6]   M   = failed megaconsistency check
    [7]   N   = failed naught check
    [8]   O   = failed climatological outlier check
    [9]   R   = failed lagged range check
    [10]  S   = failed spatial consistency check
    [11]  T   = failed temporal consistency check
    [12]  W   = temperature too warm for snow
    [13]  X   = failed bounds check
    [14]  Z   = flagged as a result of an official Datzilla
                investigation

    Args:
        element_types(list): list of sought after element types, only values
                             for these types are returned.
        filepath(str): filepath for the csv with the NOAA climate data.

    Yields:
        A three tuple with [0]station, [1]element type, [3]data dictionary
    """
    flag_codes = {
        "": 0, "D": 1, "G": 2, "I": 3, "K": 4, "L": 5, "M": 6, "N": 7, "O": 8,
        "R": 9, "S": 10, "T": 11, "W": 12, "X": 13, "Z": 14
    }
    conversions = {"TMAX": 0.1, "TMIN": 0.1, "TAVG": 0.1, "PRCP": 0.1,
                  "SNWD": 1, "SNOW": 1, "EVAP": 0.1}
    logger.debug("reading %s for data types: %s",
                 filepath, ', '.join(codes))
    with open(filepath, 'r') as current_file:
        for line in current_file:
            station, date_, element_type, value, _, flag, _, time_ = \
                line.strip('\n').split(',')

            if time_:
                date_time = datetime.datetime(
                    *[int(x) for x in [date_[:4], date_[4:6], date_[6:8],
                                       int(time_[:2]) % 24, time_[2:]]]
                )
            else:
                date_time = datetime.datetime(
                    *[int(x) for x in [date_[:4], date_[4:6], date_[6:8]]])
            if element_type in codes:
                conversion = conversions[element_type]
                value = None if delete_values else float(value) * conversion
                yield station, element_type, {
                    "datetime": date_time.isoformat() + 'Z',
                    "value": value,
                    "flag": flag_codes[flag]
                }

# DEPRECATED! / Refactor for creating locations /
# def parse_headers(elem_type, param_units,
#                   ghcnd_stations_filepath='ghcnd-stations.txt'):
#     # ID            1-11   Character
#     # LATITUDE     13-20   Real
#     # LONGITUDE    22-30   Real
#     # ELEVATION    32-37   Real
#     # STATE        39-40   Character
#     # NAME         42-71   Character
#     # GSN FLAG     73-75   Character
#     # HCN/CRN FLAG 77-79   Character
#     # WMO ID       81-85   Character
#     headers = {}
#     station_locations = {}
#     print('parsing headers', elem_type)
#     with open(ghcnd_stations_filepath, 'r') as stations_txt:
#         for line in stations_txt:
#             id = line[:11].strip(' ')
#             lat = float(line[12:20])
#             lon = float(line[21:30])
#             name = line[41:71].strip(' ')
#             code = "NOAA_" + id + "#" + elem_type
#             stationName="NOAA_" + id
#             station_locations[stationName] = (stationName, (lon, lat))
#             headers[id] = pixml.header(
#                 locationId=code,
#                 parameterId=param_units['parameterId'],
#                 stationName=stationName,
#                 lat=lat,
#                 lon=lon,
#                 units=param_units['units'])
#     return headers, station_locations


# DEPRECATED! / Refactor for creating locations /
# def to_pixml(file_path_source, file_path_target, element_types=ELEMENT_TYPES,
#              element_type_units=ELEMENT_TYPE_UNITS):
#     for element_type in element_types:
#         print('Creating pixml for', element_type)
#         values = read_file(element_type, file_path_source)
#         headerdicts, _ = parse_headers(element_type,
#                                     element_type_units[element_type])
#         pixml.create(headerdicts, values,
#                      filename=file_path_target + element_type + ".xml",
#                      timeZone=0.0)

def load_historical_data(
        first_year, last_year, data_dir="data", codes=CONFIG['codes'],
        units=CONFIG['units'], break_on_error=False):
    """Load all NOAA data from first_year to last_year."""
    for year in range(first_year, last_year + 1):
        # get data csv
        filename = str(year) + ".csv.gz"
        file_path = ftp.grab_file(ftp_url="ftp.ncdc.noaa.gov",
                                  filename=filename,
                                  unzip_tar=False)
        parse_file(file_path, data_dir, codes, units,
                   break_on_error)
    if last_year == datetime.date.today().year:
        command.touch_config()


def superghcnd_filelist():
    available_files = ftp.listdir(ftp_dir='/pub/data/ghcn/daily/superghcnd/',
                                  ftp_url="ftp.ncdc.noaa.gov",
                                  config=CONFIG)

    start = datetime.datetime.strptime(CONFIG['last_value_timestamp'],
                                       '%Y-%m-%d')
    date_range = (
        ((start + datetime.timedelta(days=y)).strftime('%Y%m%d'),
         (start + datetime.timedelta(days=y + 1)).strftime('%Y%m%d'))
        for y in range((datetime.datetime.now() - start).days + 1)
    )
    files = ('superghcnd_diff_{f}_to_{t}.tar.gz'.format(f=from_, t=to_)
             for from_, to_ in date_range)
    for file in files:
        if not file in available_files:
            try:
                raise FileNotFoundError('File %s not found on ftp.' % file)
            except FileNotFoundError:
                logger.exception('File %s not found on ftp.', file)
                raise
        yield file


def grab_recent(codes=CONFIG['codes'], data_dir="data",
                element_type_units=CONFIG['units'],
                break_on_error=False):
    for file in superghcnd_filelist():
        file_paths = ftp.grab_file(
            file,
            ftp_url="ftp.ncdc.noaa.gov",
            ftp_dir='/pub/data/ghcn/daily/superghcnd/')
        for file_path in file_paths:
            logger.debug('Parsing file: %s', file)
            delete_values = 'delete' in file_path
            if delete_values:
                logger.debug('deleting from: %s', file_path)
            parse_file(file_path, data_dir, codes, element_type_units,
                       break_on_error, delete_values=delete_values)
    command.touch_config()


def main():
    args = command.argparser(CONFIG)
    logger.info('################## START NOAA ##################')

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
                             data_dir="data",
                             codes=args.codes,
                             units=CONFIG['units'],
                             break_on_error=False)
        return
    elif last_value_days_till_now < 365:
        logger.info('Collecting data from %s till now',
                    CONFIG['last_value_timestamp'])
        grab_recent(args.codes)
    else:
        logger.info('Nothing done.')


if __name__ == "__main__":
    main()
