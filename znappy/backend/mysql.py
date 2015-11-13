""" Package for managing MySQL Backend
"""

__all__ = ['load_handlers']


from znappy.utils import config, register_handler
from fabric.api import task, local, env
import logging
import MySQLdb

logger = logging.getLogger(__name__)

env.host_string = 'localhost'

@task
def stop_mysql():
    return local('service mysql stop').return_code == 0, ""


@task
def start_mysql():
    return local('service mysql start').return_code == 0, ""


def lock_mysql():
    logger.debug('Locking tables')
    
    query('FLUSH LOCAL TABLES WITH READ LOCK;')

    return True, ""


def unlock_mysql():
    logger.debug('Unlocking tables')
    query('UNLOCK TABLES;')
    
    return True, ""


def stop_replication():
    logger.debug('Stopping replication')
    query('STOP SLAVE SQL_THREAD;')

    return True, ""


def start_replication():
    logger.debug('Starting replication')
    query('START SLAVE SQL_THREAD;')

    return True, ""


def flush_tables():
    logger.debug('Flushing tables')
    query('FLUSH TABLES;')

    return True, ""


def _connect():
    return MySQLdb.connect(
        host=config.get('host', 'localhost'),
        user=config.get('user', 'root'),
        passwd=config.get('password', None)
    )


def query(query):
    mysql = _connect()

    result = mysql.cursor().execute(query)

    mysql.close()

    return result


def load_handlers(_config, keystore):
    global config

    config = _config

    logger.debug('called with config: {}'.format(config))

    # agent
    #register_handler("start_snapshot", lock_mysql)
    #register_handler("end_snapshot", unlock_mysql)
    register_handler("start_snapshot", stop_replication, priority=0)
    register_handler("start_snapshot", flush_tables, priority=1)
    register_handler("end_snapshot", start_replication)

    # restore
    register_handler("start_restore", stop_mysql)
    register_handler("end_restore", start_mysql)
