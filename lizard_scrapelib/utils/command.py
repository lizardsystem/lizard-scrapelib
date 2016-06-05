import argparse
import datetime
import json
import logging
import os


FILE_BASE = os.path.join(os.path.dirname(__file__), '..', '..')


def setup_logger(name):
    logger = logging.getLogger(name)
    print_logger = logging.StreamHandler()
    print_logger.setLevel(logging.DEBUG)
    logger.addHandler(print_logger)
    return logger


logger = setup_logger(__name__)


def read_config(name):
    name = name.split(r'.')[-1]
    with open(os.path.join(FILE_BASE, 'var/config/{name}_config.json'.format(
            name=name)), 'r') as cfg:
        return json.load(cfg)


def touch_config():
    config = read_config()
    config['last_value_timestamp'] = datetime.date.today().strftime('%Y-%m-%d')
    with open(os.path.join(FILE_BASE, 'var/config/noaa_config.json'), 'w') as \
            cfg:
        return json.dump(config, cfg)


def store_to_dict(dict_name='dict_', dict_key_name='key', keep_dict_key=True):
    """Decorator to store result of a function to a dictionary."""
    def store_to_dict_decorator(function_):
        def store_to_dict_wrapper(*args, **kwargs):
            dict_ = kwargs[dict_name]
            try:
                key = kwargs[dict_key_name]
                if not keep_dict_key:
                    del kwargs[dict_key_name]
            except KeyError:
                key = tuple(kwargs[x] for x in dict_key_name)
                if not keep_dict_key:
                    for x in dict_key_name:
                        del kwargs[x]
            del kwargs[dict_name]
            result = function_(*args, **kwargs)
            dict_[key] = result
            return result
        return store_to_dict_wrapper
    return store_to_dict_decorator


def argparser(config):
    codes = ",".join(config.get('codes', []))
    login = config.get('login', {})
    parser_args = {
        'verbose': {
            'args': ['-v', '--verbose'],
            'kwargs': {
                "help": "Show debug logging",
                "action": "store_true"
            }},
        'lizard_backend': {
            "args": ['-b', '--lizard_backend'],
            "kwargs": {
                "help": "Lizard backend the data will be stored to. Either "
                        "'production' or 'staging' or a full lizard backend "
                        "url (without the api/v2 part). Defaults to "
                        "production.",
                "default": "production"
            }},
        "start": {
            "args": ['-S', '--start'],
            "kwargs": {
                "help": "Starting date (isoformat with dashes: 2015-03-07) "
                        "of period of the data to be collected. If this is "
                        "not set, default behaviour is to fill the database "
                        "based on the last known value for a fixed set of "
                        "timeseries. If the database hasn't been filled "
                        "before, the start date of the GPM dataset will be "
                        "chosen: 2015-03-07",
                "default": None
            }},
        "end": {
            "args": ['-E', '--end'],
            "kwargs": {
                "help": "Last date (isoformat with dashes: 2015-03-07) of "
                        "period of the data to be collected. Defaults to "
                        "today.",
                "default": datetime.date.today().isoformat()
            }},
        "start_year": {
            "args": ['-s', '--start_year'],
            "kwargs": {
                "help": "Starting year of period of the data to be collected. "
                        "If this is not set, default behaviour is to fill the "
                        "database based on the last known value for a fixed "
                        "set of timeseries. If the database hasn't been "
                        "filled before, the start year 2000 will be chosen.",
                "default": None
            }},
        "end_year": {
            "args": ['-e', '--end_year'],
            "kwargs": {
                "help":"Last year of period of the data to be collected. If "
                       "this is not set, defaults to this year.",
                "default": datetime.date.today().year
            }},
        "code_types": {
            "args": ['-t', '--code_types'],
            "kwargs": {
                "help": "Element types to be fetched. Seperate by comma. " \
                        "Possible types: {}. Default is all types.".format(
                        codes),
                "default": codes
            }},
        "pixml": {
            "args": ['-X', '--pixml'],
            "kwargs": {
                "help": "Creates PiXML from data, but doesn't store data in "
                        "the Lizard Backend.",
                "default": None
            }},
        "password": {
            "args": ['-p', '--password'],
            "kwargs": {
                "help": "Password for the Lizard API.",
                "default": None
            }},
        "username": {
            "args": ['-u', '--username'],
            "kwargs": {
                "help": "Username for the Lizard API.",
                "default": None
            }},
        "ftp_password": {
            "args": ['-P', '--ftp_password'],
            "kwargs": {
                "help": "Password for the ftp server.",
                "default": None
            }},
        "ftp_username": {
            "args": ['-U', '--ftp_username'],
            "kwargs": {
                "help": "Username for the ftp server.",
                "default": None
            }},
        "organisation": {
            "args": ['-o', '--organisation'],
            "kwargs": {
                "help": "Organisation for the Lizard API.",
                "default": login.get('organisation', "")
            }},
        "lat": {
            "args": ['-L', '--lat'],
            "kwargs": {
                "help": "Latitude for location.",
                "default": None
            }},
        "lon": {
            "args": ['-l', '--lon'],
            "kwargs": {
                "help": "Longtitude for location.",
                "default": None
            }},
        "name": {
            "args": ['-N', '--name'],
            "kwargs": {
                "help": "Name for the Lizard object.",
                "default": ""
            }},
        "code": {
            "args": ['-C', '--code'],
            "kwargs": {
                "help": "Organisation code for the lizard object.",
                "default": ""
            }},
        "endpoint": {
            "args": ['-E', '--endpoint'],
            "kwargs": {
                "help": "Lizard api endpoint",
                "default": None
            }},
        "access_modifier": {
            "args": ['-M', '--access_modifier'],
            "kwargs": {
                "help": "Lizard api access modifier for creating a Lizard "
                        "object. Defaults to 100.",
                "default": 100
            }},
        "data_dir": {
            "args": ['-D', '--data_dir'],
            "kwargs": {
                "help": "path of the data_directory",
                "default": config.get('data_dir', 'data')
            }},
    }

    parser = argparse.ArgumentParser(
        description=config.get('commandline_args', {}).get('description')
    )

    commandline_args = config.get('commandline_args', {}).get('args', ())
    for argument in commandline_args:
        parse_args = parser_args[argument]
        parser.add_argument(*parse_args['args'], **parse_args["kwargs"])

    args = parser.parse_args()

    if 'password' in commandline_args and 'username' in commandline_args:
        if not args.password or not args.username:
            args.password = login.get('password', '')
            args.username = login.get('username', '')

    if 'ftp_password' in commandline_args and 'ftp_username' in \
            commandline_args:
        if not args.ftp_password or not args.ftp_username:
            args.ftp_password = login.get('ftp_password', '')
            args.ftp_username = login.get('ftp_username', 'anomymous')

    if 'lizard_backend' in commandline_args:
        if args.lizard_backend != 'production':
            if args.lizard_backend == 'staging':
                config['lizardbase'] = 'https://nxt.staging.lizard.net'
            elif args.lizard_backend.startswith(r'https://'):
                config['lizardbase'] = args.lizard_backend
            else:
                try:
                    raise TypeError('Url for Lizard backend is of the '
                                    'incorrect format: %s' %
                                    args.lizard_backend)
                except:
                    logger.exception('Url for Lizard backend is of the '
                                     'incorrect format: %s',
                                     args.lizard_backend)
                    raise

    if 'verbose' in commandline_args:
        if args.verbose:
            debuglevel = logging.DEBUG
        else:
            debuglevel = logging.INFO

        log_filename = os.path.join(FILE_BASE, "var/log/sturing.log")
        logging.basicConfig(
            filename=log_filename,
            level=debuglevel,
            format='%(asctime)s %(levelname)s: %(message)s'
        )

        logger.debug('log_filename is: %s', log_filename)

    if 'code_types' in commandline_args:
        args.codes = args.code_types.split(',')

        if any(code not in config['codes'] for code in args.codes):
            try:
                raise TypeError('Invalid element type found in element types: '
                                '%s', args.codes)
            except TypeError:
                logger.exception('Invalid element type found in element '
                                 'types: %s', args.codes)
                raise
    if 'start_year' in commandline_args and args.start_year is not None:
        args.start_year = int(args.start_year)
    if 'last_year' in commandline_args and args.end_year is not None:
        args.end_year = int(args.end_year)

    return args
