from abc import ABCMeta, abstractmethod
from fabric.api import env, output

import logging

logger = logging.getLogger(__name__)

class BaseSnapshot:
    __metaclass__ = ABCMeta

    
    def __init__(self, keystore, config = None, name = None):
        logger.debug(config)
  
        self.keystore   = keystore

        # configure fabric
        env.host_string = 'localhost'
        env.warn_only   = True

        for c in ['running', 'stderr', 'status', 'warning']:
            output[c] = False

        if config:
            self._loaded    = False
            self.filesystem = config['filesystem']
            self.config     = config
            self.name       = None
        else:
            data = keystore.list_snapshots(name=name)

            if len(data) == 0:
                raise KeyError('Snapshot with name {} not found'.format(name))

            self._loaded    = True
            self.time       = data[0]['time']
            self.filesystem = data[0]['filesystem']
            self.config     = data[0]
            self.name       = name


    def __repr__(self):
        return "base_snapshot"
#        return str({
#            'time': self.time,
#            'filesystem': self.filesystem,
#            'name': self.name
#        })


    def save(self):
        if not self._loaded:
            logger.error('Cannot save snapshot, no snapshot loaded!')
            return False

        logger.debug('saving snapshot')

        return self.keystore.add_snapshot(
            filesystem=self.filesystem,
            name=self.name,
            time=self.time
        )


    @abstractmethod
    def create(self):
        pass


    @abstractmethod
    def list(self):
        pass


    @abstractmethod
    def restore(self):
        pass
