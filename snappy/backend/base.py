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


    def runner(self):
        # execute the pre_snapshot function
        self.pre_snapshot()

        # execute the snapshot function
        self.snapshot()

        # execute the post_snapshot function
        self.post_snapshot()
