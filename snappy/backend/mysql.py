from snappy.backend.base import BaseBackend

class MySQLBackend(BaseBackend):
    def __init__(self, config):
        pass


    def __enter__(self):
        print "pre_snapshot"


    def __exit__(self, type, value, tb):
        print "post_snapshot"


