from abc import ABCMeta, abstractmethod

class BaseBackend:
    __metaclass__ = ABCMeta


    @abstractmethod
    def start_snapshot(self):
        pass


    @abstractmethod
    def end_snapshot(self):
        pass

