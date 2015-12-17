"""
Usage:
    znappy monitor
"""
from znappy import Znappy


def main(db, args):
    znappy = Znappy(db, args['--cluster'])

    exit_code, message = znappy.monitor()

    print message
    exit(exit_code)
