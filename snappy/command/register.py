"""
Usage:
        snappy register <cluster>
"""

from snappy import utils
import logging

logger = logging.getLogger(__name__)


def main(db, args):
    config = utils.config

    logger.debug(args['<cluster>'])

    # create the config key
    db.put(
        'service/snappy/{0}/{1}/snapshots/'.format(
            args['<cluster>'],
            db.node
        ),
        None
    )
