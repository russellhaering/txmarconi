"""
Copyright 2013 Russell Haering.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from setuptools import setup, find_packages

from txmarconi.version import __version__

setup(
    name='txmarconi',
    version=__version__,
    packages=find_packages(),
    install_requires=[
        'treq >= 0.2',
        'txKeystone == 0.1.2',
    ],
    author='Russell Haering',
    author_email='russellhaering@gmail.com',
    description='A Twisted client for OpenStack Marconi: https://wiki.openstack.org/wiki/Marconi',
    license='Apache License (2.0)',
    url='https://github.com/russellhaering/txmarconi'
)
