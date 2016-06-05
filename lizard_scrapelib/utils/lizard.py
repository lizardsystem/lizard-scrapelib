import time
import urllib.error

import lizard_connector

try:
    from utils import command
except ImportError:
    from lizard_scrapelib.utils import command


logger = command.setup_logger(__name__)


def endpoint(config, name):
    return lizard_connector.connector.Endpoint(
        username=config['login']['username'],
        password=config['login']['password'],
        base=config['lizardbase'],
        endpoint=name)


def find_location(location_name, config, organisation_code):
    """
    Finds a timeseries location based on a station and element_type.

    Assumes all timeseries locations are already present in Lizard. We are not
    able to create new assets in Lizard (we are able to create locations
    however, but work from the locations allready).

    Returns:
        The lizard api location uuid.
    """
    # create connector
    timeseries_location = endpoint(config, 'locations')
    try:
        # query for station in locations and element type
        result = timeseries_location.download(name=location_name)
        result = [r for r in result if
                  r['organisation_code'] == organisation_code]
        try:
            # multiple results are (should) not (be) possible
            return result[0].get('uuid')
        except IndexError:
            logger.debug('Could not find location with %s and organisation '
                         'code part: %s', location_name, organisation_code)
            return None
    except urllib.error.HTTPError:
        return None


@command.store_to_dict(dict_name='timeseries_uuids',
                       dict_key_name=('location_name', 'timeseries_name'))
def find_timeseries(location_name, timeseries_name, config):
    """
    Find timeseries for a station and element type.

    Returns:
        The Lizard api timeseries uuid for a given station and element type.
    """
    # create timeseries connector
    timeseries = endpoint(config, 'timeseries')
    try:
        # query for station and element type
        result = timeseries.download(location__name=location_name)
        if config.get('organisation_code_filter', True):
            result = [r for r in result if
                      r['location']['organisation_code'] == timeseries_name]
        try:
            # multiple results are (should) not (be) possible
            return result[0].get('uuid')
        except IndexError:
            # No timeseries found yet, log it, and create a new one.
            logger.debug('Could not find timeseries for location %s and '
                         'organisation code: %s', location_name,
                         timeseries_name)
            pass
    except urllib.error.HTTPError:
        pass
    # find the location to create a new timeseries for
    location_uuid = find_location(location_name, config, timeseries_name)
    if location_uuid:
        try:
            timeseries_uuid = create_timeseries(
                config, location_name, location_uuid, timeseries_name=timeseries_name,
                unit=config['units'][timeseries_name.split('#')[-1]]['parameterId'])
            return timeseries_uuid
        except urllib.error.HTTPError:
            logger.exception("Could not create timeseries with location_name "
                             "= %s, timeseries_name = %s and location_uuid = %s",
                             location_name, timeseries_name, location_uuid)
            return None
    logger.info('Timeseries not created for location %s and timeseries name: '
                '%s', location_name, timeseries_name)
    return None


def create_timeseries(config, location_name, location_uuid, timeseries_name,
                      unit=None, access_modifier=100, supplier=None,
                      value_type=1):
    timeseries = endpoint(config, 'timeseries')
    timeseries_header = {
        "name": timeseries_name,
        "location": location_uuid,
        "organisation_code": timeseries_name,
        "access_modifier": access_modifier,
        "supplier_code": timeseries_name,
        "supplier": supplier,
        "parameter_referenced_unit": unit,
        "value_type": value_type
    }
    # create new timeseries:
    logger.info('Creating new timeseries with location name: %s and name: %s',
                location_name, timeseries_name)
    new_timeseries_info = timeseries.upload(data=timeseries_header)
    timeseries_uuid = new_timeseries_info['uuid']
    logger.info('New timeseries created with uuid: %s', timeseries_uuid)
    return timeseries_uuid


def create_location(config, location_name, code, lon=None, lat=None,
                    geometry=None, access_modifier=100,
                    ddsc_show_on_map=False):
    if not geometry and lat and lon:
        geometry = lizard_connector.queries.wkt_point(lon, lat)
    location = endpoint(config, 'locations')
    location_data = {
        "name": location_name,
        "organisation": config['login']['organisation'],
        "organisation_code": location_name + ("#" + code) if code else '',
        "geometry": geometry,
        "access_modifier": access_modifier,
        "ddsc_show_on_map": ddsc_show_on_map,
    }
    logger.info('Creating new timeseries location with name: %s and code: %s',
                location_name, code)
    location_info = location.upload(data=location_data)
    uuid = location_info['uuid']
    logger.info('New timeseries created with uuid: %s', uuid)
    return uuid


def find_timeseries_uuids(config):
    uuids = {}
    timeseries = endpoint(config, 'timeseries')
    timeseries.max_results = 10000000
    timeseries.all_pages = False
    added = 0
    for result in timeseries.download(
            location__name__startswith=config['name'].upper(), page_size=2500):
        uuids.update(
            {(x['location']['name'], x['name']): x['uuid'] for x in result})
        count = int(timeseries.count)
        added = max(2500 + added, count)
        logger.debug('Collecting timeseries uuids. %s done',
                     "{:5.1f}%".format(added / count))
    return uuids


def upload_timeseries_data(config, timeseries_data, break_on_error=False):
    """
    Uploads a year of data into the Lizard-api for given element types.

    Args:
        config(dict): dictionary from config json. See var/config for example
            json, or load config through:
            lizard_scrapelib.utils.command.read_config(name)
        timeseries_data(iterable): iterable of the form
                                   ((
                                      location_name,
                                      code,
                                      {
                                         'datetime': iso formatted datetime,
                                         'value': value
                                      }
                                   ), ... )
                                   optionally the data also has a 'flag' value.

    """
    timeseries = endpoint(config, 'timeseries')
    timeseries_uuids = find_timeseries_uuids(config)

    for location_name, timeseries_name, data in timeseries_data:
        # get uuid for timeseries and store uuid to dict.
        code = timeseries_name.split('#')[-1]
        uuid = timeseries_uuids.get(
            (location_name, timeseries_name),
            find_timeseries(location_name=location_name,
                            timeseries_name=timeseries_name,
                            config=config,
                            timeseries_uuids=timeseries_uuids))
        if uuid:
            try:
                logger.info('location %s | code %s | uuid %s has data: %s',
                            location_name, code, uuid, str(data))
                reaction = timeseries.upload(uuid=uuid, data=[data])
                logger.info('location %s | code %s responds after sending '
                            'data: %s',location_name, code, str(reaction))
            except urllib.error.HTTPError:
                logger.exception(
                    'Error in data found when submitting timeseries '
                    'data. Station: %s, element_type: %s',
                    location_name, code)
                time.sleep(10)
                if break_on_error:
                    raise


def lizard_create_commands():
    config = command.read_config('noaa')
    config['commandline_args']['args'] = [
        "verbose",
        "endpoint",
        "username",
        "password",
        "organisation",
        "name",
        "lizard_backend",
        "code",
        "lat",
        "lon",
        "access_modifier"
    ]
    args = command.argparser(config)
    if args.endpoint:
        if args.endpoint == "locations":
            logger.debug('Creating location at %s with:\n'
                         '  organisation code: %s#%s\n'
                         '  lon: %s,\n'
                         '  lat: %s,\n'
                         '  access modifier: %s',
                         args.lizard_backend, args.name, args.code, args.lon,
                         args.lat, args.access_modifier)
            location_uuid = create_location(
                config, args.name, args.code, lon=args.lon, lat=args.lat,
                access_modifier=args.access_modifier)
            logger.info('Created timeseries location %s with uuid %s in '
                        'backend %s', args.name, location_uuid,
                        args.lizard_backend)
        elif args.endpoint == "timeseries":
            unit = config['units'][args.code]["parameterId"]
            location_uuid = find_location(args.name, config, args.code)
            logger.debug('Creating timeseries at %s with:\n'
                         '  location uuid: %s\n'
                         '  organisation code: %s#%s\n'
                         '  unit: %s,\n'
                         '  access modifier: %s',
                         args.lizard_backend, location_uuid, args.name,
                         args.code, unit, args.access_modifier)
            timeseries_uuid = create_timeseries(
                config,
                location_name=args.name,
                location_uuid=location_uuid,
                timeseries_name=(args.name + "#" + args.code) if
                                args.code else args.name,
                unit=unit,
                access_modifier=args.access_modifier)
            logger.info('Created timeseries %s with uuid %s and location uuid '
                        '%s in backend %s', args.name, timeseries_uuid,
                        location_uuid, args.lizard_backend)
