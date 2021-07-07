# Bio-Index

Bio-Index is a tool that indexes genomic data stored in [AWS S3][s3] "tables" (typically generated by [Spark][spark]) so that it can be rapidly queried and loaded. It uses a [MySQL][mysql] database to store the indexes and to look up where in [S3][s3] each "record" is located.

The Bio-Index has two entry points: a CLI used for basic CRUD operations and a simple HTTP server and REST API for pure querying.

## Setup

First clone the git repository:

```bash
$ git clone https://github.com/broadinstitute/dig-bioindex.git
```

Then, `cd` into the directory created and install it using setup tools:

```bash
$ python ./setup.py install
```

At this point, the BioIndex is installed on your system and you can run it with the `bioindex` command.

Alternately, if you do not want to install BioIndex, you can just run it locally with `python -m main`:

```bash
$ python -m bioindex.main [--env-file <environment overrides>] <command> [args]
```

## Configuring the BioIndex

The bio-index uses [python-dotenv][dotenv] (environment variables) for configuration. There are two environment files of importance: `.bioindex` and `.env`. The `.bioindex` file contains environment variables for connecting to AWS if they need to differ from those in the AWS credentials file. If you pass `--env-file` _before the command_ you can override which environment file is used instead of `.bioindex`.

The following are the environment variables that can be set in the `.bioindex` file:

```ini
BIOINDEX_S3_BUCKET       # S3 bucket to index/read from
BIOINDEX_RDS_SECRET      # AWS SecretID used to connect to the RDS instance (*)
BIOINDEX_RDS_INSTANCE    # RDS instance name; used if no secret specified (*)
BIOINDEX_RDS_USERNAME    # RDS instance login; used if no secret specified (**)
BIOINDEX_RDS_PASSWORD    # RDS instance credentials; used if no secret specified (**)
BIOINDEX_BIO_SCHEMA      # RDS MySQL schema for the bio index (default=bio)
BIOINDEX_PORTAL_SCHEMA   # RDS MySQL schema for the portal (optional)
BIOINDEX_LAMBDA_FUNCTION # Lambda function that can be used for indexing remotely (optional)
BIOINDEX_GRAPHQL_SCHEMA  # File the GraphQL schema is written to and read from (optional)
BIOINDEX_GENES_URI       # Location of a GTF gene source (default=genes/genes.gff.gz)
BIOINDEX_RESPONSE_LIMIT  # Number of bytes to read from S3 per request (default=2 MB)
BIOINDEX_MATCH_LIMIT     # Number of matches to return per request (default=100)

(*)  - Either BIOINDEX_RDS_SECRET or BIOINDEX_RDS_INSTANCE is required
(**) - If BIOINDEX_RDS_INSTANCE is used, then username and password are required
```

Additionally, one can set a single environment variable (`BIOINDEX_ENVIRONMENT`), which should be the name of an AWS secret. If set, the BioIndex will read that secret as JSON and expects it to contain the rest of the environment setup.

Likewise, the environment can be overridden. The priority of values is as such:

```
secret < .bioindex < envrionment
```

For example, consider the following setup:

* `BIOINDEX_ENVIRONMENT` contains "bio-index-secret", which sets `BIOINDEX_S3_BUCKET` to "bio-index"
* `BIOINDEX_S3_BUCKET` is set in `.bioindex` to "bio-index-dev"

When run, the S3 bucket will be set to "bio-index-dev". Likewise, if the command line is run like so:

```bash
$ BIOINDEX_S3_BUCKET=bio-test bioindex query gene SLC30A8
```

The S3 bucket used will be "bio-test".

_NOTE: The only environment variable that **must** be set are `BIOINDEX_S3_BUCKET` and either the `BIOINDEX_RDS_SECRET` or other `BIOINDEX_RDS_*` variables. These will tell the BioIndex both where the data is located and where to write/read the index data._

## Creating Indexes

To create a new index, use the `create` command. Example:

```bash
$ bioindex create my-index prefix/key/to/files/ phenotype,chrom:pos
```

The above would create a new (or overwrite the existing) index named `my-index`. It indicates that all the files to index in the S3 bucket are located in `prefix/key/to/files/` recursively, and that the schema used to index the files should be done by `phenotype` first and then by locus: `chrom:pos`.

The "prefix" to the files should always be a directory name and end with `/`. Every object in S3 under it will be indexed, no matter how deeply nested.

The "schema" parameter for the index controls how each record is indexed. Every schema follows the same the same general format: `keys,...,locus`. Consider the following JSON record:

```json
{
    "varId": "8:117962623:C:T",
    "dbSNP": "rs769898168",
    "chromosome": "8",
    "position": 117962623,
    "phenotype": "T2D",
    "pValue": 0.39,
    "beta": 0.3,
    "consequence": "splice_region_variant",
    "gene": "SLC30A8",
    "impact": "LOW"
}
```

This record, may be indexed many different ways. For example:

* By variant ID: `varId`
* By dbSNP: `dbSNP`
* By variant ID or dbSNP: `varId|dbSNP`
* By position: `chromosome:position`
* By phenotype, then position: `phenotype,chromosome:position`
* By gene, then phenotype: `gene,phenotype`
* ...

The rules of indexing are as follows:

* Key columns can only be cardinal values and are matched exactly.
* Interchangeable keys may be separated with `|`.
* Locus must be last.
* Locus must be a position (`chr:pos`), region (`chr:start-stop`), or field template (`varId=$chr:$pos`) where the field can be parsed as a position/region, but is matched exactly by the field value as if it were a key column.

## Preparing S3 Objects

Once everything is setup, you can begin creating or preparing the objects in [S3][s3] to be indexed. Each objects is expected to be in [JSON-lines][json-lines] format, and _must be sorted in order they are to be indexed!_ The only exception to this would be if the index is always a 1:1 mapping with a single record (e.g. indexing by ID).

For example, if the the schema `phenotype,chromosome:position` is used, then the objects in [S3][s3] are expected to be written (using [Spark][pyspark]) like so:

```python
df.orderBy(['phenotype','chromosome','position']) \
    .write \
    .json('s3://my-bucket/folder')
```

The above code would write out many part files to the bucket/path, each perfectly sorted and ready to be indexed using the `index` CLI command.

## Indexing

Once an index has been created, simply use the `index` command and pass long a comma-separated list of indexes to build.

```bash
$ bioindex index my-index,another-index
```

_NOTE: You can also pass `*` as to build all indexes!_

You can also build indexes "remotely" using an AWS Lambda Function. To do this, see the [DIG Indexer][indexer] project, which is a [Serverless][serverless] project that can be used to deploy a Lambda Function to AWS. Once deployed, set the `BIOINDEX_LAMBDA_FUNCTION` environment variable and pass `--use-lambda` on the CLI for the `index` command. You can also adjust the number of workers (`--workers`) to use, which is the number of Lambda functions that will execute in parallel.

## Querying Indexes

Once you've built an index, you can then query it and retrieve all the records that match various input keys and/or overlap the given region. For example, to query all records in the `genes` key space that overlap a given region:

```bash
$ bioindex query genes chr3:983248-1180000
{'chromosome': '3', 'end': 1445901, 'name': 'CNTN6', 'source': 'symbol', 'start': 1134260, 'type': 'protein_coding'}
```

_NOTE: If you'd like to limit the output, just pipe it to `head -n`._

In addition to querying, there are also commands to `count` records, fetch `all` records, and `match` keys. Examples:

```bash
$ bioindex count genes 8:100000000-200000000
1587

$ bioindex match gene SLC30A
SLC30A1
SLC30A10
SLC30A2
SLC30A3
SLC30A4
SLC30A5
SLC30A6
SLC30A7
SLC30A8
SLC30A9
```

_NOTE: The `count` command is an approximation. It reads the first 500 records and divides the total number of bytes to read from S3 by the average byte size per record._

# The GraphQL REST Server

In addition to a CLI, Bio-Index is also a [FastAPI][fastapi] server that allows you to query records using [GraphQL][graphql] via REST calls.

## Building the GraphQL Schema

[GraphQL][graphql] requires a schema to process queries. The schema is inferred from the data, and build with the `build-schema` CLI option:

```
$ bioindex build-schema --save
```

If you don't pass `--save`, then the schema is simply printed out. By default it is written to the filename specified by the `BIOINDEX_GRAPHQL_SCHEMA` environment variable (defaulted to `schema.graphql`), but you can change the destination by either providing `--out <filename>` or simply redirecting the output somewhere else.

Once the schema has been saved, you can then start the server.

## Starting the Server

The server is started using the `serve` command:

```bash
$ bioindex serve --port 5000
```

## REST Queries

The entire REST API can be explored both via the [demo page](http://localhost:5000/) and via the REST API [documentation page](http://localhost:5000/docs).

Each request results in a JSON response that looks like so:

```json
{
    "continuation": null,
    "nonce": "Ox4YfcJapxGYST_siDYjFtp150BZEMqC5JdyTuyTMUQ",
    "count": 1,
    "page": 1,
    "data": [
        {
            "chromosome": "8",
            "end": 100728,
            "ensemblId": "ENSG00000254193",
            "name": "AC131281.2",
            "start": 100584,
            "type": "processed_pseudogene"
        }
    ],
    "index": "genes",
    "limit": null,
    "profile": {
        "query": 0.138009,
        "fetch": 0.417972
    },
    "progress": {
        "bytes_read": 368,
        "bytes_total": 368
    },
    "q": [
        "chr8:100000-101000"
    ]
}
```

The `count` is the total number of records returned by this request.

The `data` is the array of records (if `format=row`) or a dictionary of columns (if `format=column`).

The `profile` shows how long the index query took vs. how much time was spent fetching the records from [S3][s3].

The `progress` shows how many bytes were read from S3 this request and what the total number of bytes that need to be read are.

If the `continutation` value is non-null, then it is a string, which is a token indicating there are more bytes left to be read and records left to be returned. They can be retrieved using the `/api/bio/cont?token=<token>` end-point.

If the `continuation` is followed to download more records, then the `page` count is increased each subsequent call.

# Using Docker

In the `image/` subfolder is a `Dockerfile` that can be used to build a [Docker][docker] image. Or a pre-built image can be pulled from [DockerHub][hub].

## Building the Image

To build the image from scratch, run the following:

```bash
$ docker build -t broadinstitute/bioindex:latest image
```

Once built, running `docker images` should show it ready for use.

## Executing Using Docker

When running the BioIndex from the docker image, it's best to pass the environment data through with `--env-file` and if you want to make use of the GraphQL API, then a volume needs to be mounted that will point to where the `BIOINDEX_GRAPHQL_SCHEMA` file is located.

```bash
$ # list all indexes
$ docker run --env-file ./my-bioindex.env -rm broadinstitute/bioindex bioindex list

$ # build the schema and output it to stdout
$ docker run --env-file ./my-bioindex.env -v .:. -rm broadinstitute/bioindex bioindex build-schema

$ # start the server
$ docker run --env-file ./my-bioindex.env -v .:. -rm broadinstitute/bioindex bioindex serve
```

## Genes URI

When executing queries, it's often more convenient to use a gene name instead of trying to pass a specific region. Since gene names are specific to species and assemblies, the gene names are configurable using a [GFF3][gff] file. This can be a local file (and by default is the one located in this repository), but can also be a remote, hosted file. It is expected that the `attributes` column contains either the `ID` or `Name` field set to the gene name to use. If the `Alias` attribute is also present, it is assumed to be a comma-separated list of alternate names for the gene, and those will also be included in the map.

Here is an example of the first few lines of the default GTF in this repository:

```
19  .  protein_coding  58856544  58864865  .  +  .  Name=A1BG
10  .  protein_coding  52559169  52645435  .  +  .  Name=A1CF
12  .  protein_coding  9220260   9268825   .  +  .  Name=A2M
12  .  protein_coding  8975068   9039597   .  +  .  Name=A2ML1
1   .  protein_coding  33772367  33786699  .  +  .  Name=A3GALT2
```

_NOTE: It's important that GTF files tab-delimited! The spacing shown above is only for readability._

The GFF file is only downloaded/parsed if needed. It is loaded on-demand (only once per execution) if a query requiring a locus is provided something other than a known, region format (e.g. `chromosome:start-end`) and then assumes what was provided should be interpreted as a gene name.

# fin.

[python]: https://www.python.org/
[setuptools]: https://setuptools.readthedocs.io/en/latest/
[dotenv]: https://saurabh-kumar.com/python-dotenv/
[mysql]: https://www.mysql.com/
[s3]: https://docs.aws.amazon.com/AmazonS3/latest/dev/Welcome.html
[emr]: https://aws.amazon.com/emr/
[click]: https://click.palletsprojects.com/en/7.x/quickstart/
[rich]: https://rich.readthedocs.io/en/latest/
[fastapi]: https://fastapi.tiangolo.com/
[graphql-core]: https://graphql-core-3.readthedocs.io/en/latest/
[graphql]: https://graphql.org/
[uvicorn]: https://www.uvicorn.org/
[pydantic]: https://pydantic-docs.helpmanual.io/
[boto3]: https://aws.amazon.com/sdk-for-python/
[sqlalchemy]: http://www.sqlalchemy.org/
[pymysql]: https://pymysql.readthedocs.io/en/latest/
[spark]: https://spark.apache.org/
[pyspark]: https://spark.apache.org/docs/latest/api/python/pyspark.html
[json-lines]: http://jsonlines.org/examples/
[aiofiles]: https://pypi.org/project/aiofiles/
[docker]: https://docker.com/
[hub]: https://hub.docker.com/repository/docker/broadinstitute/dig-bioindex
[gff]: http://gmod.org/wiki/GFF3
