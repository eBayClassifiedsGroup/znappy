from snappy.utils import logger
from fabric.api import env, local, output, task, settings
import shutil
import yaml
from tempfile import mkdtemp

class GITStorage(BaseStorage):

    def __init__(self, **config):
        self.config = config

        env.host_string = 'localhost'
        env.warn_only   = True

        for c in ['running', 'stderr', 'status', 'warning']:
            ouput[c] = False

    def save():
        print env
        pass
