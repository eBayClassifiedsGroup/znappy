"""
Usage:
    znappy cluster list [options]
    znappy cluster restore [options]
    znappy cluster --help

Options:
    --minute=<m>        Minutes ago to search for [default: 15]
    --cluster=<name>    Name of the cluster [default: default]
"""

from znappy import models, utils
import logging
import time
import sys
import getpass
from datetime import datetime, timedelta
from prettytable import PrettyTable
from fabric.api import env, run, local, settings, sudo, task, hide
from fabric.colors import green, red


logger = logging.getLogger(__name__)


def verify_credentials():
    env.host_string = 'localhost'
    with settings(warn_only=True):
        return sudo('/bin/true')


def list_snapshots(cluster, t):
    snapshots = {}

    for h in cluster.hosts:
        snapshots_before_time = filter(lambda s: s.time < (t - 3), cluster.hosts[h].snapshots)
        if snapshots_before_time:
            snapshot = sorted(snapshots_before_time, key=lambda s: s.time, reverse=True)[0]

        snapshots[h] = snapshot

    return snapshots


def snapshot_table(snapshots, t = int(time.time())):
    table = PrettyTable(fields=['host', 'snapshot', 'time', 'lag'])
    
    for host in snapshots:
        snapshot = snapshots[host]
        lag      = int(t-snapshot.time)

        table.add_row([host, snapshot.name, snapshot.time, lag])

    return table

def action_list(cluster, t):
    snapshots = list_snapshots(cluster, t)

    table = snapshot_table(snapshots, t)

    return table.get_string(sortby='host')


def action_restore(cluster, t):
    snapshots = list_snapshots(cluster, t)

    print snapshot_table(snapshots, t).get_string(sortby='host')

    master = sorted(snapshots.values(), key=lambda s: s.time, reverse=True)[0].host

    print "Master host will be: {}".format(master.name)

    choice = raw_input("Are you sure? [y/N]: ").lower()
    if choice != 'y':
        return "Aborted!"

    env.user      = raw_input('[ldap] username: ')
    env.password  = getpass.getpass('[sudo] password for {}: '.format(env.user))

    if not verify_credentials():
        return 'Failed to verify credentials'

    # lock the whole cluster
    while not cluster.lock():
        time.sleep(0.5)

    # time to bork the sjit
    try:
        for snapshot in snapshots:
            env.host_string = snapshot.host
            sudo('znappy snapshot restore {0} --cluster={1}'.format(snapshot.name, cluster.name))
    # ye it should be that easy
    except Exception, e:
        print e.message
        return "Failed to restore cluster! :S"

    cluster.release()

    return "cluster restore complete"


def main(db, args):
    module  = sys.modules[__name__]
    cluster = models.Cluster(args['--cluster'])

    time_before = datetime.today() - timedelta(minutes=int(args['--minute']))
    time_before = int(time.mktime(time_before.timetuple()))
    result  = ""

    for c in ['list', 'restore']:
        funcname = 'action_{}'.format(c)
        if args[c] and hasattr(module, funcname):
            action = getattr(module, funcname)
            result = action(cluster, time_before)

    print result
