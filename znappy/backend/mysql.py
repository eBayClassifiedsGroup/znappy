""" Package for managing MySQL Backend
"""

from fabric.api import task, local, settings, hide
from ConfigParser import ConfigParser
import logging
import MySQLdb
import MySQLdb.cursors
import os

__all__ = ['load_handlers']

logger = logging.getLogger(__name__)


class MySQL(object):
    def __init__(self, config):
        self.config = config
        self.mycnf = self.load_mycnf(
            config.get('mycnf_path', '/etc/mysql/debian.cnf'),
            config.get('mycnf_section', 'client')
        )

    def connect(self):
        if hasattr(self, 'conn') and self.conn.open:
            return True

        logger.debug('No open MySQL connection, setting up new')
        self.conn = MySQLdb.connect(
            host='localhost',
            user=self.mycnf.get('user', 'root'),
            passwd=self.mycnf.get('password', None),
            cursorclass=MySQLdb.cursors.DictCursor
        )

    def load_mycnf(self, path, section):
        mycnf = {}

        try:
            parser = ConfigParser(allow_no_value=True)
            parser.read(os.path.expanduser(path))

            # Get the username and password
            for k in ['user', 'password']:
                mycnf[k] = parser.get(section, k)
        except:
            logger.warn('Tried to read mysql configuration from {} but failed to do so'.format(path))
            pass

        return mycnf

    def __del__(self):
        """On dereference try to destroy connection"""
        try:
            self.close()
        except:
            pass

    def close(self):
        self.conn.close()

        return True, ""

    @task
    def stop_mysql(self):
        with settings(hide('running', 'stdout')):
            result = local('service mysql stop')

        return result.return_code == 0, "stop_mysql"

    @task
    def start_mysql(self):
        with settings(hide('running', 'stdout')):
            return local('service mysql start --skip-slave-start').return_code == 0, "start_mysql"

    def i_am_master(self):
        read_only = self.query('SHOW GLOBAL VARIABLES LIKE "read_only"')[0]['Value']
        if read_only != 'OFF':
            logger.debug("Host is read-only, so it isn't the master")
            return False
        slave_status = self.query('SHOW SLAVE STATUS')
        if slave_status:
            logger.debug("Host is a slave to {}, so it isn't the master".format(slave_status[0]['Master_Host']))
            return False
        slaves = self.query('SHOW SLAVE HOSTS')
        if not slaves:
            logger.debug("Host doesn't have any slaves, so it probably isn't the master")
            return False
        return True

    def lock_mysql(self):
        logger.debug('Locking tables')
        self.query('FLUSH LOCAL TABLES WITH READ LOCK')
        return True, ""

    def unlock_mysql(self):
        logger.debug('Unlocking tables')
        self.query('UNLOCK TABLES')
        return True, ""

    def stop_replication(self):
        logger.debug('Stopping replication')
        self.query('STOP SLAVE SQL_THREAD')
        return True, ""

    def start_replication(self):
        logger.debug('Starting replication')
        self.query('START SLAVE SQL_THREAD')
        return True, ""

    def flush_tables(self):
        logger.debug('Flushing tables')
        self.query('FLUSH TABLES')
        return True, ""

    def query(self, query):
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def stop(self):
        return self.stop_mysql(self)

    def start(self):
        return self.start_mysql(self)

    def check_master(self):
        if self.i_am_master():
            return False, "Host is a master, no snapshots will be created"
        return True, ""

    def start_snapshot(self):
        return self.lock_mysql()

    def monitor(self, *args, **kwargs):
        if self.i_am_master():
            return False, (0, "OK: Host is a MySQL master")
        else:
            return True, (0, "OK: Host is a MySQL slave")

    def end_snapshot(self):
        return self.unlock_mysql()

    def failover(self, *args, **kwargs):
        cred_file = self.config.get('failover_creds', '/etc/mysql/failover.cnf')
        master = kwargs.get('master_host')
        if not master:
            return False, "No master_host given"

        with settings(hide('running')):
            return local("/usr/bin/mysqlmaster.py switch --new-master {} --defaults-extra-file={} "
                         "--dead-master --assume-yes".format(master, cred_file)).return_code == 0, ""


def load_handlers(config, register):
    mysql = MySQL(config)
    # agent
    register("pre_snapshot", mysql.check_master)
    register("start_snapshot", mysql.start_snapshot)
    register("end_snapshot", mysql.end_snapshot)
    register("post_snapshot", mysql.close)

    # restore
    # register("pre_cluster_restore", mysql.methodname)
    register("start_restore", mysql.stop)
    register("end_restore", mysql.start)
    register("post_cluster_restore", mysql.failover)

    # Monitor
    register("monitor", mysql.monitor)
