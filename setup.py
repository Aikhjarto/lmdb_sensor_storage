#!/usr/bin/env python

from distutils.core import setup

setup(name='lmdb_sensor_storage',
      version='0.1',
      description='Efficient storage for sensor data using lmdb as backend',
      author='Thomas Wagner',
      author_email='wagner-thomas@gmx.at',
      url='https://github.com/Aikhjarto/lmdb_sensor_storage',
      scripts=['scripts/check_lmbd_sensor_data_age.py'],
      include_package_data=True,
      package_data={'lmdb_sensor_storage': ['**/*.service', '**/*.timer'], }
      )
