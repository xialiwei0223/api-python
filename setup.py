from setuptools import setup, find_packages
import os
readme="DolphinDB Python API"
if os.path.exists('README.txt'):
      with open('README.txt') as f:
          readme = f.read()
setup(name='DolphinDB_API',
      version='0.6',
      author='DolphinDB, Inc.',
      author_email='support@dolphindb.com',
      license='DolphinDB',
      description='DolphinDB API',
      long_description=readme,
      packages=find_packages())