""" Monitoring package for znappy

"""

import time


def snapshot_age(znappy, *args, **kwargs):
    snapshots = sorted(znappy.host.snapshots.values(), key=lambda s: s.time, reverse=True)
    last_snapshot = snapshots[0]

    lag = (int(time.time()) - znappy.config.get('snapshot', {}).get('min_age', 3600)) - last_snapshot.time

    if lag < 5:
        return True, (0, "OK: snapshot age is less then 10 seconds")
    elif lag < 30:
        return False, (1, "WARNING: snapshot age is between 10 and 30 seconds")
    else:
        return False, (2, "CRITAL: snapshot lag is more then 30 seconds!")


def snapshot_count(znappy, *args, **kwargs):
    count = len(znappy.host.snapshots)
    rotate =  znappy.config.get("snapshot", {}).get("rotate", 12)

    if count < rotate:
        return False, (1, "WARNING: snapshot count is lower then the rotation ({}/{})".format(count, rotate))
    if count > (rotate + 1):
        return False, (2, "CRITAL: snapshot count is higher then rotation! ({}/{})".format(count, rotate))

    return True, (0, "OK: snapshot count is ok")


def load_handlers(config, snapshot, register=None):
    register("monitor", snapshot_age, priority=100)
    register("monitor", snapshot_count, priority=101)
