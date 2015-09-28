"""
    Usage: snappy-agent [options]

Options:
    -c FILE, --config FILE  Configuration file to use [default: ./config-sample.yaml]
"""

from . import backend, keystore, lockagent, utils

from docopt import docopt
import logging
import time
import yaml


def main():
    utils.get_logging('snappy-agent')

    args = docopt(__doc__)

    utils.logger.debug("Using arguments: {0}".format(args))

    config = utils.get_config(args['--config'])

    utils.logger.debug("Using configuration: {0}".format(config))


