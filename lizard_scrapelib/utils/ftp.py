import ftplib
import os

import pysftp

try:
    from pyftpclient import PyFTPclient
except ImportError:
    from lizard_scrapelib.pyftpclient import PyFTPclient

try:
    import handlers
    import command
except ImportError:
    try:
        from utils import handlers
        from utils import command
    except ImportError:
        from lizard_scrapelib.utils import handlers
        from lizard_scrapelib.utils import command


logger = command.setup_logger(__name__)


def upload_file(file_path, ftp_url, ftp_dir, username="anonymous",
                password=""):
    logger.debug('Storing: %s to %s', file_path, ftp_url)
    with pysftp.Connection(ftp_url, username=username, password=password) \
            as sftp:
        with sftp.cd(ftp_dir):
            sftp.put(file_path)


def grab_file(filename, ftp_url, ftp_dir, port=21, username="anonymous",
              password="", download_dir='data', unzip_gzip=True,
              unzip_tar=True, encoding="utf-8"):
    """Grabs file from ftp and ungzips it."""
    logger.info('grabbing from ftp %s', filename)
    file_path = os.path.join(command.FILE_BASE, download_dir, filename)
    ftp_client = PyFTPclient(ftp_url, port, username, password)
    ftp_client.DownloadFile(filename, file_path, ftp_dir)
    logger.debug('file grabbed from ftp, unzipping')
    if unzip_gzip:
        file_path = handlers.ungzip(file_path, remove=True, encoding=encoding)
        if unzip_tar:
            file_path = handlers.untar(file_path, download_dir)
        logger.debug('unzipped file: %s', str(file_path))
    return file_path


def listdir(ftp_url, ftp_dir, config):
    """
    Lists files in an ftp directory.

    Args:
        ftp_dir(str): ftp directory to be listed.
        ftp_url(str): ftp url for the ftp server where the directory is found.
    Returns:
        List with filenames found in an ftp directory.
    """
    cfg = config.get('ftp', {'username': 'anonymous', 'password': ''})
    ftp = ftplib.FTP(ftp_url)
    ftp.login(cfg['username'], cfg['password'])
    ftp.cwd(ftp_dir)
    files = []
    try:
        files = ftp.nlst()
    except ftplib.error_perm as resp:
        if str(resp) == "550 No files found":
            logger.error("No files in this directory")
        else:
            raise
    ftp.close()
    return files
