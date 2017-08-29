import json

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine(user: str, password: str, host: str, port: int, connect_args: dict) -> Engine:
    """
    :param user: MySQL user name
    :param password: MySQL password
    :param host: MySQL server host
    :param port: MySQL server port
    :param connect_args: connect_args passed to sqlalchemy (e.g. `dict(ssl=dict(ca='...', cert='...', key='...'))`)
    """
    protocol = 'mysql'
    connect_string = '{}://{}:{}@{}:{}/'.format(protocol, user, password, host, port)
    eng = create_engine(connect_string, connect_args=connect_args)
    return eng


def engine_from_json_file(config_path: str) -> Engine:
    """
    Given a path to a JSON file with MySQL configurations, return a sqlalchemy engine object
    """
    with open(config_path) as f:
        config = json.load(f)
    return get_engine(**config)