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
        'aiofiles>=25.1.0',
        'botocore>=1.43.6,<1.44',
        'boto3>=1.43.6,<1.44',
        'click>=8.1.7',
        # fastapi 0.124.x requires starlette<0.51; fastapi 0.125+ requires
        # pydantic>=2.7 and would break our pydantic 1.10.x pin.
        'fastapi>=0.124,<0.125',
        'starlette>=0.50,<0.51',
        'graphene>=3.4.3,<4',
        'graphql-core>=3.2.8,<3.3',
        'orjson>=3.11.6',
        # pydantic 2.x is a breaking change; stay on the 1.10.x line.
        'pydantic>=1.10.26,<2',
        'pymysql>=1.1.1',
        'python-dotenv>=1.2.2',
        'pyyaml>=6.0.1',
        'requests>=2.33.0',
        'rich>=13.7.1',
        'smart_open>=7.6.1,<8',
        'sqlalchemy>=2.0.49,<2.1',
        'typing-extensions>=4.15.0',
        'uvicorn[standard]>=0.46.0',
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
