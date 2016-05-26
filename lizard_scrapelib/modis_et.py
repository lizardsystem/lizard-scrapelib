import os
import ftplib

from osgeo import gdal
import lizard_scrapelib.pyftpclient
import numpy as np


def hdf_subdataset_extraction(hdf_file, dst_dir, subdataset=0):
    """
    unpack a single subdataset from a HDF5 container and write to GeoTiff
    taken from:
    http://gis.stackexchange.com/questions/174017/extract-scientific-layers-from-modis-hdf-dataeset-using-python-gdal
    """
    # open the dataset
    hdf_ds = gdal.Open(hdf_file, gdal.GA_ReadOnly)
    band_ds = gdal.Open(hdf_ds.GetSubDatasets()[subdataset][0], gdal.GA_ReadOnly)

    # read into numpy array
    band_array = band_ds.ReadAsArray().astype(np.int16)

    # convert no_data values
    band_array[band_array == -28672] = -32768

    # build output path
    band_path = os.path.join(dst_dir, os.path.basename(os.path.splitext(hdf_file)[0]) + "-sd" + str(subdataset+1) + ".tif")

    # write raster
    out_ds = gdal.GetDriverByName('GTiff').Create(band_path,
                                                  band_ds.RasterXSize,
                                                  band_ds.RasterYSize,
                                                  1,  #Number of bands
                                                  gdal.GDT_Int16,
                                                  ['COMPRESS=LZW', 'TILED=YES'])
    out_ds.SetGeoTransform(band_ds.GetGeoTransform())
    out_ds.SetProjection(band_ds.GetProjection())
    out_ds.GetRasterBand(1).WriteArray(band_array)
    out_ds.GetRasterBand(1).SetNoDataValue(-32768)
    out_ds = None  #close dataset to write to disc


def ftplist(ftp_dir):
    ftp = ftplib.FTP('ftp.ntsg.umt.edu')
    ftp.login('anonymous', '')
    ftp.cwd(ftp_dir)
    files = []
    try:
        files = ftp.nlst()
    except ftplib.error_perm as resp:
        if str(resp) == "550 No files found":
            print("No files in this directory")
        else:
            raise
    ftp.close()
    return files


def grab_file(from_, to_, to_converted):
    print('grabbing %s', from_, to_)
    ftp_client = lizard_scrapelib.pyftpclient.PyFTPclient(
        'ftp.ntsg.umt.edu', 21, login='anonymous', passwd='')
    ftp_client.DownloadFile(from_, to_)
    print('file grabbed, transforming to geotiff')
    hdf_subdataset_extraction(to_, to_converted, subdataset=0)
    os.remove(to_)

# ftp://ftp.ntsg.umt.edu/pub/MODIS/NTSG_Products/MOD16/MOD16A2.105_MERRAGMAO/Y2000/D241/

def main():
    dirs = ("/.autofs/NTSG_Products/MOD16/MOD16A2.105_MERRAGMAO/Y" +
              str(year) + "/D" + (3 - len(str(day))) * '0' + str(day) + "/"
              for year in range(2000, 2014) for day in range(1, 365, 8))
    datadir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       '../data')
    for preffered_dir in dirs:
        files = ftplist(preffered_dir)
        print(preffered_dir)
        for tilecode in ('h25v06', 'h26v06', 'h27v06', 'h27v07', 'h28v06',
                         'h28v07', 'h28v08'):
            filename = next(f for f in files if tilecode in f)
            filepath = os.path.join(preffered_dir, filename)
            grab_file(filepath, filename, os.path.join(datadir, filename))

    # first login and go to the preferred directory.
    # for example: ftp://ftp.ntsg.umt.edu/


if __name__ == '__main__':
    main()