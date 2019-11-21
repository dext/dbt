#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup
import os

package_name = "dbt-redshift"
package_version = "0.14.4"
description = """The redshift adapter plugin for dbt (data build tool)"""

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    long_description_content_type='text/markdown',
    author="Fishtown Analytics",
    author_email="info@fishtownanalytics.com",
    url="https://github.com/fishtown-analytics/dbt",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/redshift/dbt_project.yml',
            'include/redshift/macros/*.sql',
            'include/redshift/macros/**/*.sql',
        ],
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'dbt-postgres=={}'.format(package_version),
        'psycopg2>=2.7,<2.8',
        'boto3>=1.4.4,<1.11.0',
        'botocore>=1.5.0,<1.14.0',
    ],
)
