"""
Usage:
    znappy agent
"""
from znappy import Znappy
import signal


def main(db, args):
    znappy = Znappy(db, args['--cluster'])

    # bind the signalling event handlers
    # TODO SIGHUP|SIGUSER1 -> reload config
    signal.signal(signal.SIGTERM, znappy.stop)

    znappy.daemon()
