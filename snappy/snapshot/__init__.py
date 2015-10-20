import importlib


__all__ = ["zfs", "mysql", "get"]


def get(classname, *args, **kwargs):
    m = importlib.import_module('snappy.snapshot')
    return getattr(m, classname)(*args, **kwargs)
