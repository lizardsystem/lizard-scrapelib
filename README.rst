lizard-scrapelib
================
Series of scrape libraries to fill Lizard with data.


Contains
--------

- mrcmekong: waterlevel data for the mrc mekong


Install
-------

- First install dev packages in case you haven't already::

    sudo apt-get install libffi-dev libgdal-dev libgdal1-dev libxml2-dev libxslt1-dev zlib1g-dev

- Symlink local.cfg::

    ln -s local.cfg buildout.cfg

- Run buildout with Python 3::

    python3 bootstrap.py
    bin/buildout

- Edit config files in ``var/config``. Change login info and remove ``default`` from the filenames. For example: change ``default_noaa_config.json`` to ``noaa_config.json``
Run
---

Buildout installs the script and makes them available through::

    bin/noaa
    bin/gpm
    bin/et0
    bin/mekong
    bin/lizard_create

When installing with production.cfg symlinked as buildout.cfg::

    ln -s production.cfg buildout.cfg

This will start cronjobs:

- noaa: daily
- gpm: half-hourly
- mekong: daily