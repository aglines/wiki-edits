"""Simple setup.py for Dataflow code distribution only."""
from setuptools import setup, find_packages
import sys

# Python 3.11+ has tomllib built-in, older versions need tomli
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

with open('pyproject.toml', 'rb') as f:
    pyproject = tomllib.load(f)

setup(
    name=pyproject['project']['name'],
    version=pyproject['project']['version'],
    description=pyproject['project']['description'],
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=pyproject['project']['dependencies'],
    python_requires=pyproject['project']['requires-python'],
)