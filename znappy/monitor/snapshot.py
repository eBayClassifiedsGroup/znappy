""" Monitoring package for znappy

"""

import time


def snapshot_age(znappy, *args, **kwargs):
    snapshots = sorted(znappy.host.snapshots.values(), key=lambda s: s.time, reverse=True)

    if len(snapshots) == 0:
        return False, (2, "CRITICAL: No snapshots found!")

    last_snapshot = snapshots[0]

    lag = (int(time.time()) - znappy.config.get('snapshot', {}).get('min_age', 3600)) - last_snapshot.time

    if lag < 10:
        return True, (0, "OK: last snapshot age is less than 10 seconds")
    elif lag < 30:
        return False, (1, "WARNING: last snapshot age is between 10 and 30 seconds")
    else:
        return False, (2, "CRITICAL: last snapshot lag is more than 30 seconds!")


def snapshot_count(znappy, *args, **kwargs):
    count = len(znappy.host.snapshots)
    rotate = znappy.config.get("snapshot", {}).get("rotate", 12)

    if count < rotate:
        return False, (1, "WARNING: snapshot count is lower than the rotation ({}/{})".format(count, rotate))
    if count > (rotate + 1):
        return False, (2, "CRITICAL: snapshot count is higher than rotation! ({}/{})".format(count, rotate))

    return True, (0, "OK: snapshot count is ok")


def load_handlers(config, snapshot, register=None):
    register("monitor", snapshot_age, priority=100)
    register("monitor", snapshot_count, priority=101)
