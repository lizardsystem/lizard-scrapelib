import ftplib
import os

try:
    import handlers
    import command
    from pyftpclient import PyFTPclient
except ImportError:
    try:
        from utils import handlers
        from utils import command
    except ImportError:
        from lizard_scrapelib.pyftpclient import PyFTPclient
        from lizard_scrapelib.utils import handlers
        from lizard_scrapelib.utils import command


logger = command.setup_logger(__name__)


def grab_file(filename, ftp_url, ftp_dir, username="anonymous", password="",
              download_dir='data', unzip_gzip=True, unzip_tar=True):
    """Grabs file from ftp and ungzips it."""
    logger.info('grabbing from ftp %s', filename)
    file_path = os.path.join(download_dir, filename)
    ftp_client = PyFTPclient(ftp_url, username, password)
    ftp_client.download_file(filename, file_path, ftp_dir)
    logger.debug('file grabbed from ftp, unzipping')
    if unzip_gzip:
        file_path = handlers.ungzip(file_path, remove=True)
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
