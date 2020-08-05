from setuptools import setup, find_packages
from os.path import join, dirname

setup(
    name='SpiderWalker',
    version='1.0',
    packages=find_packages(),
    long_description=open(join(dirname(__file__), 'README.txt')).read(),
    entry_points={
        'console_scripts':
            ['SpiderWalker = SpiderWalker:parse_data']
    }
)
