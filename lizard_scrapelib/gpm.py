from ftplib import FTP, error_perm
import datetime
import os
import subprocess

from secrets import FTP_USER, FTP_PASSWORD

ftp = FTP("jsimpson.pps.eosdis.nasa.gov")
ftp.login(user=FTP_USER, passwd=FTP_PASSWORD)

data_directory = os.getcwd() + '/../data/gpm/'


def strfy(*args, **kwargs):
    length = kwargs.get('length', 2)
    y = []
    for x in args:
        x = str(int(x))
        y.append('0' * (length - len(x)) + x)
    return ''.join(y)


def filelist(position, step_, delta):
    filenames = []
    start = position
    while position.month == start.month:
        day, hour, minute = position.day, position.hour, position.minute
        s = strfy(hour) + strfy(minute - 29)
        date = str(year) + strfy(month) + strfy(day)
        time_ = strfy(hour) + strfy(minute)
        elapsed = position - start
        i = strfy(elapsed.seconds / 60, length=4)
        for f in format:
            filenames.append(
                "3B-HHR-L.MS.MRG.3IMERG." + date + "-S" + s + "00-E" + time_ +
                "59." + i + ".V03E." + step_ + '.' + f
            )
        position += delta
    return filenames, position


def daterange():
    now = dt.datetime.now()
    year = now.year
    month = now.month
    for yr in range(startyear, year + 1):
        print('year:', yr)
        firstM = startmonth if yr == startyear else 1
        lastM = month if yr == year + 1 else 13
        for M in range(firstM, lastM):
            print('month:', M)
            yield yr, M


for year, month in daterange():
    new_dir = data_directory + strfy(year, length=4) + '/' + strfy(month)
    os.makedirs(new_dir, exist_ok=True)
    print(new_dir)
    os.chdir(new_dir)
    current_files = os.listdir()
    current_ftp_dir = '/data/imerg/gis/' + strfy(month)
    print('loading files from:', current_ftp_dir)
    ftp.cwd(current_ftp_dir)
    step_ = "30min"
    delta = datetime.timedelta(minutes=30)
    position = datetime.datetime.strftime(CONFIG['last_value_timestamp'])
    filenames, new_position = filelist(position, step_, delta)
    for filename in filenames:
        if filename in current_files:
            print('skipping file, file allready downloaded:', filename)
            continue
        print('loading', filename, '...')
        try:
            with open(filename, 'wb') as f:
                ftp.retrbinary('RETR {}'.format(filename), f.write)
        except error_perm:
            os.remove(filename)
            print('WARNING skipping file, file not available!:', filename)

    position = new_position


def gdal_translate_and_warp(input_tif, output_tif):
    """
    Translates input file, removes nodata values (0)s and warps to WGS84.
    """
    subprocess.call(["gdal_translate", input_tif, 'temp.tif', '-a_nodata',
                     '0'])
    os.remove(input_tif)
    try:
        os.remove(input_tif[:-3] + 'tfw')
    except FileNotFoundError:
        pass
    subprocess.call(['gdalwarp', 'temp.tif', output_tif, '-srcnodata',
                     '0', '-dstnodata', '0', '-t_srs',
                     '+proj=longlat +ellps=WGS84'])
    os.remove('temp.tif')

# 110-sftp-d05