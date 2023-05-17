# AWS batch support for bgzip and the bio-index
The core logic for this endeavor lives in [Dockerfile](./Dockerfile) and the python files in this directory. Each of the three 
*_json_files.py file contains the logic for modifying the state of the json files under an s3 path.  `compress_json_files.py` will compress
all json files recursively under a given s3 path while also leaving the original json files in place.  This means that bioindex code 
can still operate on the original json files. Before running the job associated with `delete_json_files.py` you should mark the index as 
compressed using `api/bio/bgcompress/set-compressed/{idx}` end point or a sql query  This will ensure that
the bioindex code will read from the compressed files.  Finally, if you need to backtrack `decompress_json_files.py`
will bring everything back to an uncompressed state.  


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
2. Build a docker image from Dockerfile and the other files in this director and associate that image with the specified tag (-t). Any changes to those files will require this.
3. Associate the most recently built local image with our remote repo. Value for -t in step 2 should be reused here.
4. Push the image to our remote repo.



