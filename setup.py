#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(name='lmdb_sensor_storage',
      version='0.1',
      description='Efficient storage for sensor data using lmdb as backend',
      author='Thomas Wagner',
      author_email='wagner-thomas@gmx.at',
      url='https://github.com/Aikhjarto/lmdb_sensor_storage',
      scripts=['scripts/check_lmdb_sensor_data_age.py'],
      packages=find_packages(where='src'),
      package_dir={"": "src"},
      install_requires=["lmdb",
                        "numpy>=1.7",
                        "plotly>=4.0",
                        "urllib3",
                        "paho-mqtt",
                        "PyYAML",
                        "requests",
                        "regex",
                        "typing_extensions",
                        ],
      include_package_data=True,
      package_data={'lmdb_sensor_storage': ['**/*.service', '**/*.timer'], }
      )
