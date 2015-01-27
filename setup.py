from setuptools import setup
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(name='parallelprogress',
      version='0.1',
      description='Share a progress bar between parallel processes',
      long_description=read('README.md'),
      author="Aaron O'Leary",
      author_email='dev@aaren.me',
      url='http://github.com/aaren/parallelprogress',
      py_modules=['parallelprogress'],
      )
