# AWS batch support for bgzip and the bio-index
The core logic for this endeavor lives in [Dockerfile](./Dockerfile) and [compress.py](./compress_json_files.py).  

Given an S3 path, use bgzip to compress any *.json files that live within that path while also creating .gzi index files so that data
can be read out of the compressed files without fully decompressing them.

## Infrastructure and deployment
We use CloudFormation in [bgz.yml](bgz.yml) to define what we need from AWS batch,
Elastic Container Registry, and AWS permissions.  To update infrastructure/config for AWS,
edit the CloudFormation template and then go to [AWS CloudFormation](https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks?filteringText=bgzip-batch&filteringStatus=active&viewNested=true) 
and choose update and point upload bgz.yml to AWS.  The CF template is currently deployed as a stack named bgzip-batch.

If you want to make changes to job's logic, edit main.py and from this director run these commands:
1. `aws ecr get-login-password | docker login --username AWS --password-stdin <our aws acct id>.dkr.ecr.us-east-1.amazonaws.com`
2. `docker build -t <tag-name> .`
3. `docker tag <tag-name>:latest <our aws acct id>.dkr.ecr.us-east-1.amazonaws.com/bgzip-repo`
4. `docker push <our aws acct id>.dkr.ecr.us-east-1.amazonaws.com/bgzip-repo`

Here's an explainer of those commands:
1. Authenticate with our remote AWS ECR repo, step 4 will fail if you don't do this.
2. Build a docker image from Dockerfile, main.py, and requirements.txt and associate that image with the specified tag (-t). Any changes to those files will require this.
3. Associate the most recently built local image with our remote repo. Value for -t in step 2 should be reused here.
4. Push the image to our remote repo.



