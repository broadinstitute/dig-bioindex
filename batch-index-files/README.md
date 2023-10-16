# Use AWS Batch to index files in S3
In the event that the [lambda indexer](https://github.com/broadinstitute/dig-indexer/) is not able to 
process a file within the 15 minute lamdba timeout, we can use AWS Batch to process the file.  Here's an example of how to do that: 
`python -m bioindex.main index <index_name> --use-batch --workers <concurrency>`  Each file in s3 will be
designated its own job.  Concurrency is limited by the database most likely, and max(files in index, 500) 
would probably be a good upper bound to start with.

If you need to change the logic for this process, you can edit [Dockerfile](../Dockerfile) or [index_files.py](./index_files.py) 
If you make changes to either of those files, you'll need to rebuild the docker image and push it to our remote repo.  
1. From project root (one up from this dir) `docker build -t <tag-name> .`
2. `docker tag <tag-name>:latest <our aws acct id>.dkr.ecr.us-east-1.amazonaws.com/batch-indexer-repo`
3. `docker push <our aws acct id>.dkr.ecr.us-east-1.amazonaws.com/batch-indexer-repo`

The AWS batch configuration is in [batch-indexer.yml](./batch-indexer.yml).  
