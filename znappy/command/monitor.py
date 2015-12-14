"""
Usage:
    znappy monitor
"""
from znappy import Znappy
import logging
import time

logger = logging.getLogger(__name__)


def nexit(code, msg):
    print msg

    exit(code)


def main(db, args):
    if not args['--cluster']:
        logger.fatal('No cluster name provided')
        # exit 0 so upstart will not try to respawn the process
        exit(0)

    znappy = Znappy(db, args['--cluster'])

    result = znappy.monitor()

    logger.debug(result)

    nexit(result[0], result[1])
