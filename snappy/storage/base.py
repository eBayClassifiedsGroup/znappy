from abc import ABCMeta, abstractmethod

class BaseStorage:
    __metaclass__ = ABCMeta


    def __init__(**config):
        self.config = config


    @abstractmethod
    def save(self):
        pass
