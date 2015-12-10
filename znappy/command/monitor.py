"""
Usage:
    znappy monitor [<INTERVAL>] [options]

Options:
    --nagios  Use nagios output and exitcodes
"""
from znappy import Znappy
import logging
import time

logger = logging.getLogger(__name__)
znappy = None

def nexit(code, msg):
    print msg

    exit(code)


def snapshot_age():
    global znappy

    logger.debug(znappy)
    logger.debug(znappy.host.snapshots.keys())

    snapshots = sorted(znappy.host.snapshots.values(), key=lambda s: s.time, reverse=True)
    last_snapshot = snapshots[0]

    logger.debug(last_snapshot)

    lag = (int(time.time()) - znappy.config.get('snapshot', {}).get('min_age', 3600)) - last_snapshot.time

    if lag < 5:
        return True, (0, "OK: snapshot age is less then 10 seconds")
    elif lag < 30:
        return False, (1, "WARNING: snapshot age is between 10 and 30 seconds")
    else:
        return False, (2, "CRITAL: snapshot lag is more then 30 seconds!")


def snapshot_count():
    global znappy

    return True, (0, "OK: snapshot count is ok")

def main(db, args):
    if not args['--cluster']:
        logger.fatal('No cluster name provided')
        # exit 0 so upstart will not try to respawn the process
        exit(0)

    znappy = Znappy(db, args['--cluster'])

    result = znappy.monitor()

    logger.debug(result)

    nexit(result[0], result[1])
