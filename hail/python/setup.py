#!/usr/bin/env python

from setuptools import setup, find_packages
# avoid evaluating hail/__init__.py just to get hail_pip_version
import os
import sys
sys.path.append(os.getcwd()+'/hail')
from _generated_version_info import hail_pip_version

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="hail",
    version=hail_pip_version,
    author="Hail Team",
    author_email="hail-team@broadinstitute.org",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://hail.is",
    packages=find_packages(),
    package_data={'': ['hail-all-spark.jar']},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.6",
    install_requires=[
        'numpy<2',
        'pandas>0.22,<0.24',
        'matplotlib<3',
        'seaborn<0.9',
        'bokeh<0.14',
        'pyspark>=2.2,<2.3',
        'parsimonious<0.9',
        'ipykernel<5',
        'decorator<5',
    ]
)
