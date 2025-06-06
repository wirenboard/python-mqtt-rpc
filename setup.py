#!/usr/bin/env python
import os

from setuptools import find_packages, setup


def get_version():
    with open("debian/changelog", "r", encoding="utf-8") as f:
        return f.readline().split()[1][1:-1].split("~")[0]


def read(fname):
    try:
        path = os.path.join(os.path.dirname(__file__), fname)
        return open(path, encoding="utf-8").read()
    except IOError:
        return ""


setup(
    name="mqttrpc",
    version=get_version(),
    packages=find_packages(),
    # metadata for upload to PyPI
    author="Evgeny Boger",
    author_email="boger@contactless.ru",
    maintainer="Wiren Board Team",
    maintainer_email="info@wirenboard.com",
    url="https://github.com/wirenboard/python-mqtt-rpc",
    description="WB MQTT-RPC reference implementation",
    long_description=read("README.md"),
    # Full list:
    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    license="MIT",
)
