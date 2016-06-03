from setuptools import setup

version = '0.1dev'

long_description = '\n\n'.join([
    open('README.rst').read(),
    open('CREDITS.rst').read(),
    open('CHANGES.rst').read(),
    ])

install_requires = [
    'celery',
    'ciso8601',
    'lizard-connector',
    'lxml',
    'setuptools',
    'rasterstats',
    ],

setup(name='lizard-scrapelib',
      version=version,
      description="Series of scrape libraries to fill Lizard with data.",
      long_description=long_description,
      # Get strings from http://www.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[
          'Topic :: Software Development :: Libraries :: Application Frameworks'
      ],
      keywords=['lizard', 'rest', 'interface', 'api'],
      author='Roel van den Berg',
      author_email='roel.vandenberg@nelen-schuurmans.nl',
      url='http://demo.lizard.net',
      license='GPL',
      packages=['lizard_scrapelib'],
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      entry_points={
          'console_scripts': [
              'noaa = lizard_scrapelib.noaa:main',
              'mekong = lizard_scrapelib.mekong:main',
              'et0 = lizard_scrapelib.modis_et:main',
              'lizard_create = lizard_scrapelib.utils.lizard:lizard_create_commands'
          ]},
      )
