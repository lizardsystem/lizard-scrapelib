from calendar import Calendar
import datetime
import os

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



# Start logger and read configuration
logger = command.setup_logger(__name__)
CONFIG = command.read_config('noaa')


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
                conversion = CONFIG["conversions"][element_type]
                value = None if delete_values else float(value) * conversion
                station = "NOAA_" + station
                yield station, station + "#" + element_type, {
                    "datetime": date_time.isoformat() + 'Z',
                    "value": value,
                    "flag": CONFIG["flag_codes"][flag]
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



def parse_dly(file_path, element, from_date):
    """
    FORMAT OF DATA FILES (".dly" FILES)

    Each ".dly" file contains data for one station.  The name of the file
    corresponds to a station's identification code.  For example, "USC00026481.dly"
    contains the data for the station with the identification code USC00026481).

    Each record in a file contains one month of daily data.  The variables on each
    line include the following:

    ------------------------------
    Variable   Columns   Type
    ------------------------------
    ID            0-11   Character
    YEAR         11-15   Integer
    MONTH        15-17   Integer
    ELEMENT      17-21   Character
    VALUE1       21-26   Integer
    MFLAG1       26-27   Character
    QFLAG1       27-28   Character
    SFLAG1       29-29   Character
    VALUE2       30-34   Integer
    MFLAG2       35-35   Character
    QFLAG2       36-36   Character
    SFLAG2       37-37   Character
      .           .          .
      .           .          .
      .           .          .
    VALUE31    262-266   Integer
    MFLAG31    267-267   Character
    QFLAG31    268-268   Character
    SFLAG31    269-269   Character
    ------------------------------
    """
    total_data = []
    conversion = CONFIG["conversions"][element]
    with open(file_path, 'r') as dly_file:
        month_calendar = Calendar()
        for line in dly_file:
            element_ = line[17:21]
            year = int(line[11:15])
            month = int(line[15:17])
            if element == element_ and datetime.datetime(year, month, 1) >= \
                    from_date:
                values = [None] + [(int(line[x:x+5]) * conversion, line[x+6])
                                   for x in range(21, 269, 8)]
                total_data += [{"datetime": day.isoformat() + 'Z',
                                "value": values[day.day][0],
                                "flag": CONFIG["flag_codes"][values[day.day][1]]}
                               for day in
                               month_calendar.itermonthdates(year, month) if
                               values[day.day][0] != -9999 * conversion]
    station_name = "NOAA_" + line[:11]
    timeseries_name = station_name + "#" + element
    return station_name, timeseries_name, total_data


def load_historical_data(
        first_year, last_year, codes=CONFIG['codes'], break_on_error=False):
    locations = lizard.endpoint(CONFIG, 'locations')
    locations.max_results = 10000000
    locations.all_pages = False
    location_names = (x['name'].strip('NOAA_') for l in
                      locations.download(name__startswith="NOAA_",
                                         page_size=1000) for x in l )
    from_date = datetime.datetime(first_year,1,1)
    timeseries_uuids = lizard.find_timeseries_uuids(CONFIG)
    for location_name in location_names:
        file_path = ftp.grab_file(
            filename=location_name + '.dly',
            ftp_url="ftp.ncdc.noaa.gov",
            ftp_dir="pub/data/ghcn/daily/all",
            unzip_gzip=False,
            unzip_tar=False)
        for code in codes:
            x = [parse_dly(file_path, code, from_date)]
            lizard.upload_timeseries_data(
                CONFIG, x, timeseries_uuids, break_on_error=False)
        os.remove(file_path)


def load_historical_data_yearly(
        first_year, last_year, codes=CONFIG['codes'], break_on_error=False):
    """Load all NOAA data from first_year to last_year."""
    timeseries_uuids = lizard.find_timeseries_uuids(CONFIG)
    for year in range(first_year, last_year + 1):
        # get data csv
        filename = str(year) + ".csv.gz"
        file_path = ftp.grab_file(ftp_url="ftp.ncdc.noaa.gov",
                                  ftp_dir='/pub/data/ghcn/daily/by_year/',
                                  filename=filename,
                                  unzip_tar=False)
        iterator = read_file(codes, file_path, delete_values=False)
        lizard.upload_timeseries_data(CONFIG, iterator, timeseries_uuids,
                                      break_on_error)
        os.remove(file_path)
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


def grab_recent(codes=CONFIG['codes'], break_on_error=False):
    timeseries_uuids = lizard.find_timeseries_uuids(CONFIG)
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
            iterator = read_file(codes, file_path, delete_values=False)
            lizard.upload_timeseries_data(CONFIG, iterator, timeseries_uuids,
                                          break_on_error)
            os.remove(file_path)
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
                             codes=args.codes,
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
