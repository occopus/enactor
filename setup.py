#!/usr/bin/env -e python

import setuptools
from pip.req import parse_requirements

setuptools.setup(
    name='OCCO-Enactor',
    version='0.1.0',
    author='Adam Visegradi',
    author_email='adam.visegradi@sztaki.mta.hu',
    namespace_packages=[
        'occo',
    ],
    packages=[
        'occo.enactor',
    ],
    scripts=[
    ],
    url='http://www.lpds.sztaki.hu/',
    license='LICENSE.txt',
    description='OCCO Enactor',
    long_description=open('README.txt').read(),
    install_requires=[
        'OCCO-Compiler',
        'OCCO-InfoBroker',
        'OCCO-Util',
    ]
)
