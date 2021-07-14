from setuptools import setup

setup(
    name='bioindex',
    version='0.2',
    packages=[
        'bioindex',
        'bioindex.api',
        'bioindex.lib',
    ],
    py_modules=[
        'bioindex.main',
        'bioindex.server',
    ],
    install_requires=[
        'aiofiles>=0.6',
        'botocore>=1.20',
        'boto3>=1.17',
        'click>=7.0',
        'fastapi>=0.60',
        'graphql-core>=3.0',
        'orjson>=3.5',
        'pydantic>=1.4',
        'pymysql>=0.10',
        'python-dotenv>=0.15',
        'requests>=2.25',
        'rich>=10.0',
        'smart_open>=3.0',
        'sqlalchemy>=1.4',
        'uvicorn>=0.13',
    ],
    entry_points={
        'console_scripts': ['bioindex=bioindex.main:main'],
    },
    author='Jeffrey Massung',
    author_email='jmassung@broadinstitute.org',
    description='HuGe BioIndex',
    keywords='huge bioindex broad broadinstitute',
    url='https://github.com/broadinstitute/dig-bioindex',
    project_urls={
        'Issues': 'https://github.com/broadinstitute/dig-bioindex/issues',
        'Source': 'https://github.com/broadinstitute/dig-bioindex',
    },
    license='BSD3',
)
