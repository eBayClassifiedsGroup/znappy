from snappy.backend.base import BaseBackend
from snappy import utils
from fabric.api import task, local, env

import MySQLdb

class MySQLBackend(BaseBackend):
    @task
    def stop_mysql(self):
        return local('service mysql stop')


    @task
    def start_mysql(self):
        return local('service mysql start')


    def lock_mysql(self):
        utils.logger.debug('Locking tables')
        return self.query('FLUSH LOCAL TABLES WITH READ LOCK;')


    def unlock_mysql(self):
        utils.logger.debug('Unlocking tables')
        return self.query('UNLOCK TABLES')


    def __init__(self, config):
        env.host_string = 'localhost'

        self.config = config

        self.mysql = MySQLdb.connect(
                        host=config['host'],
                        user=config['user'],
                        passwd=config['password']
        )


    def __del__(self):
        self.mysql.close()

    def start_snapshot(self):
        # should also remove from loadbalancer
        return self.lock_mysql()


    def end_snapshot(self):
        # should also add back to the loadbalancer
        return self.unlock_mysql()


    def start_restore(self):
        return self.stop_mysql().return_code


    def end_restore(self):
        return self.start_mysql().return_code
    

    def query(self, query):
        cursor = self.mysql.cursor()
        cursor.execute(query)
