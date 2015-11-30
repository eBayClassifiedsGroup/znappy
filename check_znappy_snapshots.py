#!/usr/bin/env python
"""
Check snapshot count and latency

Usage:
    check_znappy_snapshots.py [options]
    
Options:
    -w <MINUTES>, --warn <MINUTES>      Warn time for snapshot age [default: 5]
    -c <MINUTES>, --critical <MINUTES>  Critical time for snapshot age in minute [default: 15]
    --driver <NAME>                     Name of the driver to check [default: znappy.snapshot.zfs]
"""


from docopt import docopt
from znappy import keyvalue, models
import yaml
import json

import sys

EXIT_CODES = {
    0: "OK",
    1: "WARN",
    2: "CRITICAL",
    3: "UNKNOWN"
}


def main():
    args   = docopt(__doc__)
    config = load_config()

    try:
        db = keyvalue.KeyValue(
            host=config.get('consul_host', 'localhost'),
            port=config.get('consul_port', 8500)
        )
    except:
        nagios(3, 'Could not connect to consul')

    with db:
        models.db = db

        cluster_config = load_cluster_config()

        cluster = models.Cluster(config.get('cluster', 'default'))
        host    = models.Host(db.node, cluster)

        check_snapshot_count(cluster_config, host)
        check_snapshot_age(cluster_config, host)


def check_snapshot_age(args, config, host):
    snapshots = sorted(host.snapshots, key=lambda s: s.time, reverse=True)

    if not snapshots:
        nagios(3, 'No snapshots')

    snapshot = filter(lambda s: s.driver= args['--driver'], snapshots)[0]

    msg = 'Last snapshot older then {} seconds'
    min_age = config.get('snapshot', {'min_age': 1800}).get('min_age', 1800)

    if snapshot.time < (int(time.time()) - min_age - 60*args['--critical']):
        nagios(2, msg.format())
    elif snapshot.time < (int(time.time()) - min_age - 60*args['--warn']):
        nagios(1, msg.format())
    else:
        nagios(0, 'Last snapshot is ok')

def check_snapshot_count(args, config, host):
    drivers = config.get('drivers', {})

    if not drivers or drivers.get(args['--driver']):
        nagios(3, 'No such driver')

    driver = drivers.get(args['--driver'])

    if len(host.snapshots) < driver.get('rotate', 12):
        nagios(1, 'Snapshot count ({0}) lower then {1}!'.format(len(host.snapshots), driver.get('rotate', 12)))


def nagios(ec, msg):
    print "{0}: {1}".format(EXIT_CODES[ec], msg)
    exit(ec)

def load_cluster_config():
    index, data = self.db.get('service/znappy/config')

    if data:
        return json.loads(data['Value'])
    else:
        return {}


def load_config():
    try:
        with open('/etc/znappy/config.yaml') as f:
            config =  yaml.load(f)
    except IOError:
        config = {}

    return config


if __name__ == "__main__":
    main()
