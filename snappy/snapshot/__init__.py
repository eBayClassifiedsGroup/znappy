import logging

__all__ = ["zfs", "mysql", "Snapshot"]

logger = logging.getLogger(__name__)

class Snapshot(object):
    _required = ['name', 'target', 'time']

    def _load(**kwargs):
        data = keystore.list_snapshots(**kwargs)

        if len(data) != 0:
            raise KeyError('Snapshot with {} not found'.format(kwargs))

        self.__dict__ = data[0]
        self.data     = data[0]


    def __init__(self, keystore, **kwargs):
        self._keystore = keystore

        # defaults
        if kwargs:
            self._load(kwargs)


    def save(self, **kwargs):
        self.__dict__.update(kwargs)

        # remove all internal dict items (eg _item)
        data = {k:v for (k,v) in self.__dict__.items() if k[0:1] != "_"}

        for i in self._required:
            if i not in data:
                logger.critical("Snapshot missing required attributes")
                return False

        return self._keystore.add_snapshot(data['name'], snapshot=data)
