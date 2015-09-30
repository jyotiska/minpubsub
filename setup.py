#!/usr/bin/env python

from distutils.core import setup
import sys, os, multiprocessing
import minpubsub

requires = []

py_version = sys.version_info[:2]

PY3 = py_version[0] == 3

if PY3:
    raise RuntimeError('minpubsub runs only on Python 2.6 or Python 2.7')
else:
    if py_version < (2, 6):
        raise RuntimeError('On Python 2, minpubsub requires Python 2.6 or better')
    if py_version > (2, 6):
        pass

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='minpubsub',
    version='0.2.1',
    description="A minimal PubSub messaging model with multiple persistence options - SQLite, MySQL, MongoDB",
    long_description=read('README.rst'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Telecommunications Industry",
        "License :: Free for non-commercial use",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
    ],
    keywords='pubsub publish-subscribe sqlite mongodb mysql',
    author='Jyotiska NK',
    author_email='jyotiska123@gmail.com',
    url='http://github.com/jyotiska/minpubsub',
    py_modules=['minpubsub'],
    scripts=['minpubsub.py'],
)
