import datetime
import ftplib
import gzip
import logging
import os
import socket
import threading
import time
import urllib.error

try:
    import pixml
    import import_shape
except ImportError:
    from lizard_scrapelib import pixml
    from lizard_scrapelib import import_shape
    from lizard_scrapelib.secrets import PWD, USR, NOAA_ORGANISATION

import lizard_connector
from pprint import pprint

BASE = 'https://demo.lizard.net'

FIRST_YEAR = 2000
ELEMENT_TYPES = ("TMAX", "TMIN", "TAVG", "PRCP", "SNWD", "SNOW", "EVAP")
ELEMENT_TYPE_UNITS = {
    "TMAX": {"parameterId": "WNS1923", "units": "oC"},
    "TMIN": {"parameterId": "WNS1923", "units": "oC"},
    "TAVG": {"parameterId": "WNS1923", "units": "oC"},
    "PRCP": {"parameterId": "WNS1400", "units": "mm"},
    "SNWD": {"parameterId": "WNS1400", "units": "mm"},
    "SNOW": {"parameterId": "WNS1400", "units": "mm"},
    "EVAP": {"parameterId": "VERDPG (mm)", "units": "mm"}
}


def ungzip(filename, remove=False, encoding='utf-8'):
    zipped = gzip.GzipFile(filename=filename, mode='rb')
    new_filename = filename.replace('.gz', '')
    with open(new_filename, 'w') as new_file:
        new_file.write(zipped.read().decode(encoding))
    if remove:
        os.remove(filename)
    return new_filename


def grab_files(data_dir="data", first_year=FIRST_YEAR, last_year=None):
    if not last_year:
        last_year = datetime.datetime.now().year
    filenames = (str(year) + "csv.gz" for year in range(
        first_year, last_year + 1))


def setInterval(interval, times = -1):
    # This will be the actual decorator,
    # with fixed interval and times parameter
    def outer_wrap(function):
        # This will be the function to be
        # called
        def wrap(*args, **kwargs):
            stop = threading.Event()

            # This is another function to be executed
            # in a different thread to simulate setInterval
            def inner_wrap():
                i = 0
                while i != times and not stop.isSet():
                    stop.wait(interval)
                    function(*args, **kwargs)
                    i += 1

            t = threading.Timer(0, inner_wrap)
            t.daemon = True
            t.start()
            return stop
        return wrap
    return outer_wrap


class PyFTPclient:
    def __init__(self, host, monitor_interval=30):
        self.host = host
        self.monitor_interval = monitor_interval
        self.ptr = None
        self.max_attempts = 15
        self.waiting = True


    def download_file(self, dst_filename, local_filename=None, dst_dir=None):
        res = ''
        if local_filename is None:
            local_filename = dst_filename

        with open(local_filename, 'w+b') as f:
            self.ptr = f.tell()

            @setInterval(self.monitor_interval)
            def monitor():
                if not self.waiting:
                    i = f.tell()
                    if self.ptr < i:
                        print("%d  -  %0.1f Kb/s" % (i, (i-self.ptr)/(1024*self.monitor_interval)))
                        self.ptr = i
                    else:
                        ftp.close()


            def connect():
                ftp.connect(self.host)
                ftp.login()
                if dst_dir:
                    ftp.cwd(dst_dir)
                # optimize socket params for download task
                ftp.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                ftp.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 75)
                ftp.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)

            ftp = ftplib.FTP()
            ftp.set_debuglevel(2)
            ftp.set_pasv(True)

            connect()
            ftp.voidcmd('TYPE I')
            dst_filesize = ftp.size(dst_filename)

            mon = monitor()
            while dst_filesize > f.tell():
                try:
                    connect()
                    self.waiting = False
                    # retrieve file from position where we were disconnected
                    res = ftp.retrbinary('RETR %s' % dst_filename, f.write) if f.tell() == 0 else \
                              ftp.retrbinary('RETR %s' % dst_filename, f.write, rest=f.tell())

                except:
                    self.max_attempts -= 1
                    if self.max_attempts == 0:
                        mon.set()
                        logging.exception('')
                        raise
                    self.waiting = True
                    print('waiting 30 sec...')
                    time.sleep(30)
                    print('reconnect')


            mon.set() #stop monitor
            ftp.close()

            if not res.startswith('226 Transfer complete'):
                print('Downloaded file {0} is not full.'.format(dst_filename))
                # os.remove(local_filename)
                return None

            return 1


def download_file(file_path):
    """
    Taken from: http://stackoverflow.com/questions/19692739/python-ftplib-hangs-at-end-of-transfer
    """
    print('making connection')
    ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
    ftp.login()
    ftp.set_debuglevel(2)

    # change to relevant folder
    ftp.cwd('/pub/data/ghcn/daily/by_year/')

    filename = os.path.basename(file_path)
    sock = ftp.transfercmd('RETR ' + filename)
    def background(file_path):
        f = open(file_path, 'wb')
        while True:
            block = sock.recv(1024*1024)
            if not block:
                break
            f.write(block)
        sock.close()
    t = threading.Thread(target=background, args=(file_path,))
    t.start()
    while t.is_alive():
        t.join(60)
        print('refreshing connection')
        ftp.voidcmd('NOOP')
    print('number of threads running: ', threading.active_count())
    ftp.quit()


def grab_file(file_path='data/1800.csv.gz'):
    print('grabbing %s', file_path)
    filename = os.path.basename(file_path)
    ftp_client = PyFTPclient('ftp.ncdc.noaa.gov')
    ftp_client.download_file(filename, file_path,
                             '/pub/data/ghcn/daily/by_year/')
    print('file grabbed, unzipping')
    file_path = ungzip(file_path, remove=True)
    print('file grabbed %s', file_path)


def find_location(station, element_type):
    timeseries_location = lizard_connector.connector.Endpoint(
        username=USR, password=PWD, base=BASE, endpoint="locations"
    )
    try:
        result = timeseries_location.download(name="NOAA_" + station)
        pprint(result)
        result = [r for r in result if r['organisation_code']
                  == "NOAA_" + station + "#" + element_type]
        try:
            return result[0].get('uuid')
        except IndexError:
            return None
    except urllib.error.HTTPError:
        return None


def find_timeseries(station, element_type):
    timeseries = lizard_connector.connector.Endpoint(
        username=USR, password=PWD, base=BASE, endpoint='timeseries')
    try:
        result = timeseries.download(location__name="NOAA_" + station)
        result = [r for r in result if r['location']['organisation_code'] \
                  == "NOAA_" + station + "#" + element_type]
        try:
            return result[0].get('uuid')
        except IndexError:
            pass
    except urllib.error.HTTPError:
        pass
    location_uuid = find_location(station, element_type)
    if location_uuid:
        id_ = 'NOAA_' + station + '#' + element_type
        new_timeseries_header = {
            "name": id_,
            "location": location_uuid,
            "organisation_code": id_,
            "access_modifier": 100,
            "supplier_code": "NOAA_" + element_type,
            "supplier": None,
            "parameter_referenced_unit": ELEMENT_TYPE_UNITS[element_type][
                'parameterId'],
            "value_type": 1
        }
        new_timeseries_info = timeseries.upload(data=new_timeseries_header)
        print(new_timeseries_info)
        pprint(new_timeseries_info)
        return new_timeseries_info['uuid']
    return None


def parse_year(year, data_dir="data", element_types=ELEMENT_TYPES,
               element_type_units=ELEMENT_TYPE_UNITS):
    filename = str(year) + ".csv.gz"
    file_path = os.path.join(data_dir, filename)
    print('grabbing file for %s', year)
    grab_file(file_path=file_path)

    timeseries = lizard_connector.connector.Endpoint(
        username=USR, password=PWD, base=BASE, endpoint='timeseries')

    for element_type in element_types:
        print('Parsing data for', element_type)
        all_stations_values_from_file = read_file(element_type, file_path[:-3])
        pprint(lizard_connector.queries.timeseries_values_data(all_stations_values_from_file[next(iter(all_stations_values_from_file.keys()))]))
        all_stations_values = {
            station: lizard_connector.queries.timeseries_values_data(
                station_values) for station, station_values in
            all_stations_values_from_file.items()
        }
        for station in all_stations_values.keys():
            # get uuid for timeseries
            uuid = find_timeseries(station, element_type)
            if uuid:
                print('creating timeseries for year %s for station %s', year,
                      station)
                print('uploading timeseries for year %s for station %s', year,
                      station)
                result = timeseries.upload(uuid=uuid,
                                           data=all_stations_values[station])
                pprint(result)
            else:
                print('failed for uuid %s', uuid)


def read_file(element_type, filepath):
    flag_codes = {
        "": 0, "D": 1, "G": 2, "I": 3, "K": 4, "L": 5, "M": 6, "N": 7, "O": 8,
        "R": 9, "S": 10, "T": 11, "W": 12, "X": 13, "Z": 14
    }
    values_all_stations = {}
    conversion = {"TMAX": 0.1, "TMIN": 0.1, "TAVG": 0.1, "PRCP": 0.1,
                  "SNWD": 1, "SNOW": 1, "EVAP": 0.1}[element_type]
    print("reading", filepath, element_type)
    with open(filepath, 'r') as current_file:
        for line in current_file:
            line = line.strip('\n').split(',')
            # [0] ID = 11 character station identification code
            # [1] YEAR/MONTH/DAY = 8 character date in YYYYMMDD format
            #     (e.g. 19860529 = May 29, 1986)
            # [2] ELEMENT = 4 character indicator of element type
            # [3] DATA VALUE = 5 character data value for ELEMENT
            # [4] M-FLAG = 1 character Measurement Flag
            # [5] Q-FLAG = 1 character Quality Flag
            # [6] S-FLAG = 1 character Source Flag
            # [7] OBS-TIME = 4-character time of observation in hour-minute
            #     format (i.e. 0700 =7:00 am)
            #
            # Q-FLAG CODES:
            # [0] Blank = did not fail any quality assurance check
            # [1]   D   = failed duplicate check
            # [2]   G   = failed gap check
            # [3]   I   = failed internal consistency check
            # [4]   K   = failed streak/frequent-value check
            # [5]   L   = failed check on length of multiday period
            # [6]   M   = failed megaconsistency check
            # [7]   N   = failed naught check
            # [8]   O   = failed climatological outlier check
            # [9]   R   = failed lagged range check
            # [10]  S   = failed spatial consistency check
            # [11]  T   = failed temporal consistency check
            # [12]  W   = temperature too warm for snow
            # [13]  X   = failed bounds check
            # [14]  Z   = flagged as a result of an official Datzilla
            #             investigation

            if line[7]:
                date_time = datetime.datetime(
                    *[int(x) for x in [line[1][:4], line[1][4:6], line[1][6:8],
                                       int(line[7][:2]) % 24, line[7][2:]]]
                )
            else:
                date_time = datetime.datetime(
                    *[int(x) for x in [line[1][:4], line[1][4:6], line[1][6:8]]
                      ])
            if line[2] == element_type:
                values = values_all_stations.get(line[0], [])
                values.append({
                    "datetime": date_time,
                    "value": str(int(line[3]) * conversion),
                    "flag": flag_codes[line[5]]
                })
                values_all_stations[line[0]] = values
        return values_all_stations


# station_locations = {
#     "Can Tho": ("Can_Tho", (
#         587471.5011,
#         1109089.8358))


def parse_headers(elem_type, param_units,
                  ghcnd_stations_filepath='ghcnd-stations.txt'):
    # ID            1-11   Character
    # LATITUDE     13-20   Real
    # LONGITUDE    22-30   Real
    # ELEVATION    32-37   Real
    # STATE        39-40   Character
    # NAME         42-71   Character
    # GSN FLAG     73-75   Character
    # HCN/CRN FLAG 77-79   Character
    # WMO ID       81-85   Character
    headers = {}
    station_locations = {}
    print('parsing headers', elem_type)
    with open(ghcnd_stations_filepath, 'r') as stations_txt:
        for line in stations_txt:
            id = line[:11].strip(' ')
            lat = float(line[12:20])
            lon = float(line[21:30])
            name = line[41:71].strip(' ')
            code = "NOAA_" + id + "#" + elem_type
            stationName="NOAA_" + id
            station_locations[stationName] = (stationName, (lon, lat)) # check lat lon
            headers[id] = pixml.header(
                locationId=code,
                parameterId=param_units['parameterId'],
                stationName=stationName,
                lat=lat,
                lon=lon,
                units=param_units['units'])
    return headers, station_locations


def read_files(element_types=ELEMENT_TYPES, data_dir="data",
               first_year=FIRST_YEAR, last_year=None):
    filepaths = iter(grab_files(data_dir, first_year, last_year))
    for filepath in filepaths:
        for element_type in element_types:
            for value in read_file(element_type, filepath):
                pass
        os.remove(filepath)


def to_pixml(file_path_source, file_path_target, element_types=ELEMENT_TYPES,
             element_type_units=ELEMENT_TYPE_UNITS):
    for element_type in element_types:
        print('Creating pixml for', element_type)
        values = read_file(element_type, file_path_source)
        headerdicts, _ = parse_headers(element_type,
                                    element_type_units[element_type])
        pixml.create(headerdicts, values,
                     filename=file_path_target + element_type + ".xml",
                     timeZone=0.0)


def main():
    # pprint('test grabbing file')
    # grab_file()
    # pprint('test finished')
    parse_year(2009, element_types=['EVAP'])
    parse_year(2010, element_types=['EVAP'])

    print('YEARS 2010 & 2009 EVAP HAVE BEEN PARSED')

    for year in range(2000, 2016):
        if year in (2009, 2010):
            continue
        parse_year(year, element_types=['EVAP'])
    # grab_file()
    # dd = "/home/roel/Documents/Projecten/G4AW/"
    # fn = "2015.csv"
    # fp_s = "/home/roel/Documents/Projecten/G4AW/2015.csv"
    # fp_t = "/home/roel/Documents/Projecten/G4AW/NOAA_2015"
    # to_pixml(fp_s, fp_t, element_types=['EVAP'])
    # _, station_locations = parse_headers("", param_units={"parameterId": "WNS1923", "units": "oC"},
    #               ghcnd_stations_filepath='ghcnd-stations.txt')
    # import_shape.create_measuringstation_import_zip(
    #     station_locations, asset_name="MeasuringStation", station_type=3,
    #     prefix="", frequency="daily", category="NOAA",
    #     file_path='asset_import_file2')


if __name__ == "__main__":
    main()
