from setuptools import setup

version = '0.1dev'

long_description = '\n\n'.join([
    open('README.rst').read(),
    open('CREDITS.rst').read(),
    open('CHANGES.rst').read(),
    ])

install_requires = [
    'setuptools',
    'celery',
    'lxml'
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
          ]},
      )
