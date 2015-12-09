"""
Usage:
    znappy monitor [<INTERVAL>] [options]

Options:
    --nagios  Use nagios output and exitcodes
"""
from znappy import Znappy
import logging

logger = logging.getLogger(__name__)

def nexit(code = 0, msg = None):
    if msg:
        logger.info(msg)

    if args['--nagios']:
        exit(code)
    else:
        exit(1)


def snapshot_age(znappy):
    pass


def main(db, args):
    if not args['--cluster']:
        logger.fatal('No cluster name provided')
        # exit 0 so upstart will not try to respawn the process
        exit(0)

    znappy = Znappy(db, args['--cluster'])

    logger.debug(znappy.monitor())
