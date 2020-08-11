from os.path import dirname
from os.path import join

from setuptools import find_packages
from setuptools import setup

setup(
    name="SpiderWalker",
    version="1.0",
    packages=find_packages(),
    long_description=open(join(dirname(__file__), "README.txt")).read(),
)
