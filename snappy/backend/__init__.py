from snappy.backend.mysql import MySQLBackend
import importlib


__all__ = ["mysql"]


def get(classname, *args, **kwargs):
    m = importlib.import_module('snappy.backend')
    return getattr(m, classname)(*args, **kwargs)
