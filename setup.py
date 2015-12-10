import os
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="znappy",
    version="0.1.8",
    author="Jorn Wijnands",
    author_email="jwijnands@ebay.com",
    maintainer="Jorn Wijnands",
    maintainer_email="jwijnands@ebay.com",
    description=("Tool for creating and managing (distributed) ZFS snapshots"),
    long_description=read('README.md'),
    license="GPL",
    keywords="python zfs snapshot backup",
    url="https://github.corp.ebay.com/ecg-marktplaats/so-znappy",
    packages=['znappy'] + [os.path.join("znappy", p) for p in find_packages("znappy")],
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
        'bin/znappy',
    ],
    data_files=[
        ('/etc/init/', ['etc/init/znappy-daemon.conf']),
        ('/etc/znappy/', ['etc/znappy/config.json-sample'])
    ]
)

