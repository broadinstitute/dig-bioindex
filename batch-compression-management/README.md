# AWS batch support for bgzip and the bio-index
The core logic for this endeavor lives in [Dockerfile](./Dockerfile) and the python files in this directory. Each of the three 
*_json_files.py file contains the logic for modifying the state of the json files under an s3 path.  `compress_json_files.py` will compress
all json files recursively under a given s3 path while also leaving the original json files in place.  This means that bioindex code 
can still operate on the original json files while we're in the process of compressing them. `compress_json_array.py` is the per-file variant of the same job for huge files: each child of a Batch array job compresses every Nth file of the sorted listing, where N is the array size passed as a job parameter.
Before running the job associated with `delete_json_files.py` you should mark the index as 
compressed using `python -m bioindex.main update-compressed-status <index_name> <s3_path> -c` or a sql query. 
This will ensure that the bioindex code will read from the compressed files.  Finally, if you need to backtrack `decompress_json_files.py`
will bring everything back to an uncompressed state.  

Useful cli commands:

1. `python -m bioindex.main compress <index_name> <s3_path_for_index>` compress json files for index, leaving originals in place
2. `python -m bioindex.main update-compressed-status <index_name> <s3_path_for_index> -c` mark index as compressed in the database, server will try to read from compressed files after this command
3. `python -m bioindex.main remove-uncompressed-files <index_name> <s3_path_for_index> -c` delete json files, only do this after making sure compressed files are in place and index is marked as compressed in the database.
4. `python -m bioindex.main decompress <index_name> <s3_path_for_index>` restore uncompressed json files from their compressed versions, useful if we find bugs in the compressed code paths.
5. `python -m bioindex.main compress-array <index_name> <s3_path_for_index>` compress json files for an index whose individual files are far too large for `compress` (10+ GB each). Submits the `bgzip-array-job` AWS Batch array job: one 4 vCPU child per file, multi-threaded bgzip, no per-file timeout, 3 retries per child. Leaves originals in place and skips files that already have `.gz` + `.gzi`, so it can be re-run. Exits non-zero if any child failed; only then run `remove-uncompressed-files` and `update-compressed-status`.


## Infrastructure and deployment
We use CloudFormation in [bgz.yml](bgz.yml) to define what we need from AWS batch,
Elastic Container Registry, and AWS permissions.  To update infrastructure/config for AWS,
edit the CloudFormation template and then go to [AWS CloudFormation](https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks?filteringText=bgzip-batch&filteringStatus=active&viewNested=true) 
and choose update and navigate to bgz.yml on your machine and upload to AWS.  The CF template is currently deployed as a stack named bgzip-batch.

If you want to make changes to job's logic, edit the python code or docker file in this directory and from this directory run these commands:
1. `aws ecr get-login-password | docker login --username AWS --password-stdin <our aws acct id>.dkr.ecr.us-east-1.amazonaws.com`
2. `docker build -t <tag-name> .`
3. `docker tag <tag-name>:latest <our aws acct id>.dkr.ecr.us-east-1.amazonaws.com/bgzip-repo`
4. `docker push <our aws acct id>.dkr.ecr.us-east-1.amazonaws.com/bgzip-repo`

Here's an explainer of those commands:
1. Authenticate with our remote AWS ECR repo, step 4 will fail if you don't do this.
2. Build a docker image from Dockerfile and the other files in this directory and associate that image with the specified tag (-t). Any changes to those files will require this.
3. Associate the most recently built local image with our remote repo. Value for -t in step 2 should be reused here.
4. Push the image to our remote repo.



