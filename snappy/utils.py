import logging
import yaml


logger = None


def get_config(filename):
    with open(filename) as handle:
        config = yaml.load(handle)

    return config


def get_logging(name, level=logging.DEBUG):
    global logger

    if logger is None:
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(logging.StreamHandler())

    return logger
