"""
Usage:
        znappy register <cluster>
"""

from znappy import utils
import logging

logger = logging.getLogger(__name__)


def main(db, args):
    config = utils.config

    logger.debug(args['<cluster>'])

    # create the config key
    db.put(
        'service/znappy/{0}/{1}/snapshots/'.format(
            args['<cluster>'],
            db.node
        ),
        None
    )
