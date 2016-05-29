import os
import ftplib

from osgeo import gdal
from osgeo import osr
import lizard_scrapelib.pyftpclient
import numpy as np
import rasterstats


def hdf_subdataset_extraction(hdf_file, geotiff_path, subdataset=0):
    """
    unpack a single subdataset from a HDF5 container and write to GeoTiff
    taken from:
    http://gis.stackexchange.com/questions/174017/extract-scientific-layers-from-modis-hdf-dataeset-using-python-gdal
    """
    # open the dataset
    hdf_ds = gdal.Open(hdf_file, gdal.GA_ReadOnly)
    band_ds = gdal.Open(hdf_ds.GetSubDatasets()[subdataset][0], gdal.GA_ReadOnly)

    # read into numpy array
    band_array = band_ds.ReadAsArray().astype(np.float64)

    # convert no_data values
    band_array[band_array == -28672] = -32768

    # write raster
    out_ds = gdal.GetDriverByName('GTiff')
    print(out_ds)
    geotiff_dataset = out_ds.Create(geotiff_path,
                  band_ds.RasterXSize,
                  band_ds.RasterYSize,
                  1,  #Number of bands
                  gdal.GDT_Float64,
                  ['COMPRESS=LZW', 'TILED=YES'])
    geotiff_dataset.SetGeoTransform(band_ds.GetGeoTransform())
    geotiff_dataset.SetProjection(band_ds.GetProjection())
    geotiff_dataset.GetRasterBand(1).WriteArray(band_array)
    geotiff_dataset.GetRasterBand(1).SetNoDataValue(-32768)
    geotiff_dataset = None  #close dataset to write to disc
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


def calculate_stats(tiff, shape):
    return rasterstats.zonal_stats(
        shape,
        tiff,
        stats="min max median std range",
        add_stats={"IQR": lambda x: np.subtract(*np.percentile(x, [75, 25]))}
    )


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
        #for tilecode in ('h25v06', 'h26v06'): #, 'h27v06', 'h27v07', 'h28v06', 'h28v07', 'h28v08'):
        tilecode = 'h26v06'
        filename = next(f for f in files if tilecode in f)
        filepath = os.path.join(preffered_dir, filename)
        geotiff_path = os.path.join(datadir, os.path.basename(os.path.splitext(filepath)[0]) + "-sd1.tif")
        grab_file(filepath, filename, geotiff_path)
        shape_path = os.path.join(datadir, 'Bogra', 'Bogra_upazillas.shp')


    # first login and go to the preferred directory.
    # for example: ftp://ftp.ntsg.umt.edu/


if __name__ == '__main__':
    main()