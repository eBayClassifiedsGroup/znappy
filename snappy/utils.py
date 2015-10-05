import logging
import yaml


logger = None


def get_config(filename):
    with open(filename) as handle:
        config = yaml.load(handle)

    return config


def get_logging(name, args):
    global logger

    if args['--debug']:
        level = logging.DEBUG
    else:
        level = logging.INFO

    if logger is None:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-8.8s] %(message)s"))

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)

    return logger
