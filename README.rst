lizard-scrapelib
================
Series of scrape libraries to fill Lizard with data.


Contains
--------

- mrcmekong: waterlevel data for the mrc mekong


Install
-------

- First install dev packages in case you haven't already::

    sudo apt-get install libgdal-dev
    sudo apt-get install libgdal1-dev

- Run buildout with Python 3::

    python3 bootstrap.py
    bin/buildout

- To get lxml you might need to do::

    sudo apt-get install python-dev libxml2-dev libxslt1-dev zlib1g-dev


Run with Celery
---------------

PM