from os import path
from setuptools import setup, find_packages

from app import NAME, VERSION


here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name=NAME,
    version=VERSION,

    python_requires='>=3.5',
    install_requires=[
        'boto3>=1.7.40',
        'click>=6.7',
        'python-gnupg>=0.4.3',
        'pyyaml>=3.12',
    ],

    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'tmp']),
    entry_points={'console_scripts': ['icebox=app.cli:icebox']},

    author='',
    author_email='',
    description='Encrypting command-line client for Amazon Glacier.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='',
    license='GPLv3+',
)
