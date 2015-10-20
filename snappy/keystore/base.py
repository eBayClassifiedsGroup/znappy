from abc import ABCMeta, abstractmethod

class BaseKeystore:
    __metaclass__ = ABCMeta

    def __enter__(self):
        self.connect()

        return self


    def __exit__(self, type, value, tb):
        self.close()


    @abstractmethod
    def list_snapshots(self):
        """Retrieve snapshots from the keystore, this method should return a list of snapshots"""
        return


    @abstractmethod
    def add_snapshot(self, snapshot):
        """Add an snapshot tot the keystore, this method should return a bool"""
        return


    @abstractmethod
    def get_config(self):
        """Get configuration for this host from the keystore"""
        return
