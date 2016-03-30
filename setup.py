import os
import znappy
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="znappy",
    version=znappy.__version__,
    author=znappy.__author__,
    author_email="jwijnands@ebay.com",
    description=("Tool for creating and managing (distributed) ZFS snapshots"),
    long_description=read('README.md'),
    license="MIT",
    keywords="python zfs snapshot backup",
    url="https://github.com/eBayClassifiedsGroup/znappy",
    packages=["znappy"] + [os.path.join("znappy", p) for p in find_packages("znappy")],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
        "License :: GPL License",
    ],
    install_requires=[
        "fabric",
        "python-consul",
        "python-daemon",
        "python-docopt",
        "python-mysqldb",
        "python-prettytable",
        "python-pyyaml",
    ],
    scripts=[
        "bin/znappy",
        "bin/zpyglass",
    ],
)
