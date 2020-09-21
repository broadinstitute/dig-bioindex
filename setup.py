from setuptools import setup

setup(
    name='bioindex',
    version='0.1',
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
        'aiofiles>=0.4',
        'botocore>=1.13',
        'boto3>=1.10',
        'click>=7.0',
        'fastapi>=0.53',
        'mysqlclient>=1.4',
        'orjson>=2.6',
        'pydantic>=1.4',
        'python-dotenv>=0.10',
        'requests>=2.21',
        'rich>=1.2',
        'sqlalchemy>=1.3',
        'uvicorn>=0.10',
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
