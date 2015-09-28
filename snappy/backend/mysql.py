from snappy.backend.base import BaseBackend

class MySQLBackend(BaseBackend):
    def __init__(self, config):
        pass


    def pre_snapshot(self):
        print "pre_snapshot"


    def snapshot(self):
        print "snapshot"

        return "this-would-be-the-time", "this-would-be-the-name"


    def post_snapshot(self):
        print "post_snapshot"


