import gzip
import os
import re
import tarfile


def ungzip(filename, remove=False, encoding='utf-8'):
    """Ungzips a file."""
    zipped = gzip.GzipFile(filename=filename, mode='rb')
    new_filename = re.sub('\.gz$', '', filename)
    with open(new_filename, 'w') as new_file:
        new_file.write(zipped.read().decode(encoding))
    if remove:
        os.remove(filename)
    return new_filename


def untar(file_path, download_dir):
    """Untars a file."""
    extract_dir = os.path.join(
        download_dir, re.sub('\.tar$', '', os.path.basename(file_path)))
    with tarfile.open(file_path) as tar:
        tar.extractall(path=download_dir)
    os.remove(file_path)
    return [os.path.join(extract_dir, x) for x in os.listdir(extract_dir)]
