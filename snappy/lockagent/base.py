from abc import ABCMeta, abstractmethod

class BaseLockAgent:
    __metaclass__ = ABCMeta


    def __enter__(self):
        self.connect()

        return self


    def __exit__(self, type, value, tb):
        self.close()


    @abstractmethod
    def connect(self):
        pass
    

    @abstractmethod
    def close(self):
        pass


    @abstractmethod
    def acquire(self):
        """Get a lock"""
        pass


    @abstractmethod
    def release(self):
        """Release a lock"""
        pass
