import logging
import yaml

logger = logging.getLogger(__name__)


def local_config(args={}):
    try:
        with open('/etc/znappy/client.yaml') as f:
            config = yaml.load(f)
            for k in config.keys():
                args['--{}'.format(k)] = config.get(k, False)
    except Exception:
        logger.warn('Failed to load local configuration!')
        exit(1)

    return args
