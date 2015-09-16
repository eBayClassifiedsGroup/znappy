from abc import ABCMeta, abstractmethod

class BaseLockAgent:
    __metaclass__ = ABCMeta

    
    @abstractmethod
    def acquire(self):
        """Get a lock"""
        return


    @abstractmethod
    def release(self):
        """Release a lock"""
        return
