import urllib.error

import lizard_connector

try:
    from utils import command
except ImportError:
    from lizard_scrapelib.utils import command


logger = command.setup_logger(__name__)


def loc_code(name, code):
    return name + (("#" + code) if code else '')


def endpoint(config, endpoint_type):
    return lizard_connector.connector.Endpoint(
        username=config['login']['username'], password=config['login'][
            'password'],
        base=config['lizardbase'], endpoint=endpoint_type)


def find_location(location_name, config, code=''):
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
        code_ = loc_code(location_name, code)
        result = [r for r in result if r['organisation_code'] == code_]
        try:
            # multiple results are (should) not (be) possible
            return result[0].get('uuid')
        except IndexError:
            logger.debug('Could not find location with %s and organisation '
                         'code part: %s', location_name, code)
            return None
    except urllib.error.HTTPError:
        return None


@command.store_to_dict(dict_name='timeseries_uuids',
                       dict_key_name=('location_name', 'code'))
def find_timeseries(location_name, config, code=''):
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
        code_ = loc_code(location_name, code)
        result = [r for r in result if
                  r['location']['organisation_code'] == code_]
        try:
            # multiple results are (should) not (be) possible
            return result[0].get('uuid')
        except IndexError:
            # No timeseries found yet, log it, and create a new one.
            logger.debug('Could not find timeseries for location %s and '
                         'organisation code part: %s', location_name, code)
            pass
    except urllib.error.HTTPError:
        pass
    # find the location to create a new timeseries for
    location_uuid = find_location(location_name, config, code)
    if location_uuid:
        try:
            timeseries_uuid = create_timeseries(
                config, location_name, location_uuid, code=code,
                unit=config['units'][code]['parameterId'])
            return timeseries_uuid
        except urllib.error.HTTPError:
            logger.exception("Could not create timeseries with location_name "
                             "= %s, code = %s and location_uuid = %s",
                             location_name, code, location_uuid)
            return None
    logger.info('Timeseries not created for location %s and organisation code'
                'part: %s', location_name, code)
    return None


def create_timeseries(config, location_name, location_uuid, code="", unit=None,
                      access_modifier=100, supplier=None, value_type=1):
    timeseries = endpoint(config, 'timeseries')
    id_ = location_name + '#' + code
    timeseries_header = {
        "name": id_,
        "location": location_uuid,
        "organisation_code": id_,
        "access_modifier": access_modifier,
        "supplier_code": loc_code(location_name, code),
        "supplier": supplier,
        "parameter_referenced_unit": unit,
        "value_type": value_type
    }
    # create new timeseries:
    logger.info('Creating new timeseries with name: %s and code: %s',
                location_name, code)
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
        "organisation_code": loc_code(location_name, code),
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
                config, args.name, location_uuid, args.code, unit,
                access_modifier=args.access_modifier)
            logger.info('Created timeseries %s with uuid %s and location uuid '
                        '%s in backend %s', args.name, timeseries_uuid,
                        location_uuid, args.lizard_backend)
