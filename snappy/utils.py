import logging
import yaml

config = None
logger = None

def load_config(filename):
    global config

    with open(filename) as handle:
        config = yaml.load(handle)


def load_logger(name, debug = False):
    global logger

    level = logging.DEBUG if debug else logging.INFO

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-8.8s] %(message)s"))
 
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
