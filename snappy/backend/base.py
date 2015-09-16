from abc import ABCMeta, abstractmethod

class BaseBackend:
    __metaclass__ = ABCMeta

    @abstractmethod
    def pre_snapshot(self):
        pass


    def snapshot(self):
        print "base class snapshot"


    @abstractmethod
    def post_snapshot(self):
        pass


    @staticmethod
    def runner(backend):
        print "backend_runner"
        print "got backend {0}".format(backend)
