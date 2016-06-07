from ftplib import error_perm
import calendar
import datetime
import os
import subprocess

try:
    from utils import command
    from utils import ftp
except ImportError:
    from lizard_scrapelib.utils import command
    from lizard_scrapelib.utils import ftp


CONFIG = command.read_config('gpm')
logger = command.setup_logger(__name__)


def strfy(*args, **kwargs):
    length = kwargs.get('length', 2)
    y = []
    for x in args:
        x = str(int(x))
        y.append('0' * (length - len(x)) + x)
    return ''.join(y)


def position_from_datetime(dt):
    position = datetime.datetime.strptime(CONFIG['original_position'],
                                        "%Y-%m-%dT%H:%M")
    delta = datetime.timedelta(minutes=30)
    while dt > position:
        position += delta
    return position


def filelist(position):
    # position = position_from_datetime(dt)
    delta = datetime.timedelta(minutes=30)
    filenames = []
    start = position
    year, month = position.year, position.month
    while position.month == start.month:
        day, hour, minute = position.day, position.hour, position.minute
        s = strfy(hour) + strfy(minute - 29)
        date = str(year) + strfy(month) + strfy(day)
        time_ = strfy(hour) + strfy(minute)
        elapsed = position - start
        i = strfy(elapsed.seconds / 60, length=4)
        from_file = "3B-HHR-L.MS.MRG.3IMERG." + date + "-S" + s + "00-E" + \
                    time_ + "59." + i + ".V03E.30min."
        to_file = CONFIG['lizard_slug'] + "_" + \
                  position.isoformat().replace(':', '') + "+0000.geotiff"
        filenames.append((from_file, to_file))
        position += delta
    return filenames, position


def daterange(start, end):
    start = datetime.datetime.strptime(start, '%Y-%m-%d')
    end = datetime.datetime.strptime(end, '%Y-%m-%d')
    for yr in range(start.year, end.year + 1):
        print('year:', yr)
        firstM = start.month if yr == start.year else 1
        lastM = end.month + 1 if yr == end.year else 13
        for M in range(firstM, lastM):
            print('month:', M)
            yield yr, M


def grab_and_post(args):
    this_year = datetime.date.today().year
    position = position_from_datetime(
        datetime.datetime.strptime(args.start, '%Y-%m-%d'))
    for year, month in daterange(args.start, args.end):
        logger.debug('Grabbing files for %s %d.',
                     calendar.month_name[month], year)
        os.makedirs(args.data_dir, exist_ok=True)
        if year == this_year:
            ftp_dir = '/data/imerg/gis/{month}'.format(month=strfy(month))
        else:
            ftp_dir = '/data/imerg/gis/{year}/{month}'.format(
                year=strfy(year, length=4), month=strfy(month))
        filenames, position = filelist(position)
        for from_file, to_file in filenames:
            skip = False
            for extension in ("tif.gz", "tfw.gz"):
                try:
                    ftp.grab_file(
                        from_file + extension, CONFIG['ftp_from'], ftp_dir,
                        username=args.ftp_username,
                        password=args.ftp_password,
                        download_dir=os.path.join(command.FILE_BASE,
                                                  args.data_dir),
                        unzip_tar=False,
                        encoding=None
                    )
                except error_perm as ftp_error:
                    if 'No such file or directory' in str(ftp_error):
                        logger.debug("File does not exist: %s ignoring "
                                     "error: %s", from_file, ftp_error)
                        skip = True
                if skip:
                    break
            if skip:
                continue
            output_tif = os.path.join(command.FILE_BASE,
                                      args.data_dir, to_file)
            logger.debug('Warping %stif to %s', from_file, output_tif)
            gdal_translate_and_warp(
                input_tif=os.path.join(command.FILE_BASE, args.data_dir,
                                       from_file + 'tif'),
                output_tif=output_tif
            )
            logger.debug('Uploading %s to lizard ftp.', output_tif)
            ftp.upload_file(file_path=output_tif,
                            ftp_url=CONFIG['ftp_to'],
                            ftp_dir=CONFIG['ftp_to_dir'],
                            username=CONFIG['login']['username'],
                            password=CONFIG['login']['password'])
            logger.info('File %s uploaded.', output_tif)
            os.remove(output_tif)
            command.touch_config(position.date())


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


def main():
    args = command.argparser(CONFIG)
    logger.info('################### START GPM ###################')

    if not args.start:
        args.start = CONFIG["last_value_timestamp"]

    grab_and_post(args)


if __name__ == '__main__':
    main()

