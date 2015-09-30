from abc import ABCMeta, abstractmethod

class BaseBackend:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __enter__(self):
        pass


    @abstractmethod
    def __exit__(self, type, value, tb):
        pass

