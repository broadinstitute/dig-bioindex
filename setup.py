from setuptools import setup

setup(
    name='bioindex',
    version='0.2',
    packages=[
        'bioindex',
        'bioindex.api',
        'bioindex.lib',
        'bioindex.middleware',
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
        'orjson>=3.11.6',
        'pydantic>=1.10.26,<2',
        'pymysql>=1.1.1',
        'python-dotenv>=1.2.2',
        'pyyaml>=6.0',
        'requests>=2.33.0',
        'rich>=10.0',
        'smart_open>=5.0',
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
