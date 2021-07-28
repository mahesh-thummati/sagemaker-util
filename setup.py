#!/usr/bin/env python

import os
from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as fp:
    README = fp.read()

with open(os.path.join(here, 'VERSION')) as version_file:
    VERSION = version_file.read().strip()

excluded_packages = ["docs", "tests", "tests.*"]

setup(
    name='sagemaker_util',
    version=VERSION,
    description="Utility Module for working with sagemaker and s3",
    long_description=README,
    entry_points={
        'console_scripts': ['datagen=datagen.cli:execute_from_command_line']
    },
    author='mthummati',
    author_email='maheshbabu.thummati@gmail.com',
    license='MIT License',
    packages=find_packages(exclude=excluded_packages),
    platforms=["any"],
    python_requires=">=3.6",
    install_requires=[
        "boto3"
    ]
)