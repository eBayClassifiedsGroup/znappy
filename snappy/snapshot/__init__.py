from snappy.snapshot.zfs import ZFSSnapshot
from snappy.snapshot.mysql import MySQLSnapshot
import importlib


__all__ = ["zfs", "mysql"]


def get(classname, *args, **kwargs):
    m = importlib.import_module('snappy.snapshot')
    return getattr(m, classname)(*args, **kwargs)
