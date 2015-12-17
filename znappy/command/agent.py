"""
Usage:
    znappy agent [--daemon|--force]

Options:
    -f, --force    Force create a snapshot, this bypasses the pre_/post_ phase and the locking agent
    --daemon       Daemonize the program (will not be backgrounded!)
"""
from znappy import Znappy
import logging
import signal

logger = logging.getLogger(__name__)

def main(db, args):
    znappy = Znappy(db, args['--cluster'])

    if args['--daemon']:
        # bind the signalling event handlers
        # TODO SIGHUP|SIGUSER1 -> reload config
        signal.signal(signal.SIGTERM, znappy.stop)

        znappy.daemon()
    else:
        znappy.run(force=args['--force'])
