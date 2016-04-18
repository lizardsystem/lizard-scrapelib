import datetime
import ftplib
import gzip
import os

try:
    import pixml
except ImportError:
    from lizard_scrapelib import pixml

FIRST_YEAR = 1763
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

    # connect to domain name:
    ftp = ftplib.FTP('ftp.ncdc.noaa.gov/')
    ftp.login()

    # change to relevant folder
    ftp.cwd('/pub/data/ghcn/daily/by_year/')

    # iterate over filenames
    for filename in filenames:
        file_path = os.path.join(data_dir, filename)
        with open(file_path, 'wb') as localfile:
            ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
        file_path = ungzip(file_path, remove=True)
    ftp.quit()


def read_file(element_type, filepath):
    flag_codes = {
        "": 0, "D": 1, "G": 2, "I": 3, "K": 4, "L": 5, "M": 6, "N": 7, "O": 8,
        "R": 9, "S": 10, "T": 11, "W": 12, "X": 13, "Z": 14
    }
    values_all_stations = {}
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

            try:
                date_time = datetime.datetime.strptime(
                    line[1] + line[7], "%Y%m%d%H%M")
            except ValueError:
                date_time = datetime.datetime.strptime(line[1], "%Y%m%d")
            if line[2] == element_type:
                values = values_all_stations.get(line[0], [])
                values.append({
                    "datetime": date_time,
                    "value": line[3],
                    "flag": flag_codes[line[5]]
                })
                values_all_stations[line[0]] = values
        return values_all_stations


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
    print('parsing headers', elem_type)
    with open(ghcnd_stations_filepath, 'r') as stations_txt:
        for line in stations_txt:
            id = line[:11].strip(' ')
            lat = float(line[13:20])
            lon = float(line[22:30])
            name = line[42:71].strip(' ')
            headers[id] = pixml.header(
                locationId="NOAA_" + id + "_" + elem_type,
                parameterId=param_units['parameterId'],
                stationName="NOAA_" + id,
                lat=lat,
                lon=lon,
                units=param_units['units'])
    return headers


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
        headerdicts = parse_headers(element_type,
                                    element_type_units[element_type])
        pixml.create(headerdicts, values,
                     filename=file_path_target + element_type + ".xml",
                     timeZone=0.0)


if __name__ == "__main__":
    dd = "/home/roel/Documents/Projecten/G4AW/"
    fn = "2015.csv"
    fp_s = "/home/roel/Documents/Projecten/G4AW/2015.csv"
    fp_t = "/home/roel/Documents/Projecten/G4AW/NOAA_2015"
    to_pixml(fp_s, fp_t)



# def scan_options(filepath):
#     filelength = 0
#     with open(filepath, 'r') as current_file:
#         for _ in current_file:
#             filelength += 1
#
#     pos = 0
#     next_percentage = 0
#     with open(filepath, 'r') as current_file:
#         result = [set(x) for x in next(current_file).split(',')]
#         for line in current_file:
#             pos += 1
#             if (pos / filelength) > next_percentage:
#                 print(str(next_percentage * 100) + r"%")
#                 next_percentage += 0.01
#             line = line.split(',')
#             for i, x in enumerate(line):
#
#
#                 result[i].add(x)
#         for i, x in enumerate(result):
#             print(i, x)