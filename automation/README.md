# Bio-Index Automation Scripts

The script `run-bioindex-processor-and-reindex.sh` runs the individual stages of the Bioindex Aggregator Processor (that transforms data produced by upstream Aggregator Processors into formats suitable for being indexed and served up by the Bioindex) and interleaves re-indexing operations on the resulting data.  This is done to minimize disruption to the Dev Bioindex data.  It's possible to run all the Bioindex Processor Stages up front, and re-index everything when that's done.  But that would mean that the data underlying the Dev Bioindex would be out of sync with the indexes for up to the time it takes for all the Aggregator Stages to run: 1-2 days.  Rebuilding each index after the data being referred to by the index changes means that individual indexes will only be broken or wonky for as long as it takes for their data to be re-built and re-indexed, on the order of minutes or hours, not days.

`run-bioindex-processor-and-reindex.sh` Runs from some machine, and runs commands to remotely execute the various aggregator stages on the Aggregator EC2 instance, and the Dev Bioindex EC2 instance.  This happens in detached screen sessions on the remote hosts, so that even if the host machine running `run-bioindex-processor-and-reindex.sh` dies, the remote operations will continue.  Unfortunately, it's not possible to pick up when one left off in a case like that without commenting out parts of the script.  See the `Future Work` section.

The screen sessions on the remote hosts are named `automation`, and may be re-attached to if necessary.  Output from the last operation is logged to a file in `~ec2-user/automation/`.

# Prerequisites

## Screen
Nothing special here, `screen` just needs to be available.

## SSH setup
The scripts expect the following host aliases to be configured in `~/.ssh/config`:
- `aggregator`
- `bioindex`

Pointing to the EC2 instances hosting the Aggregator and _DEV_ Bioindex installs, respectively.  Note that the scripts talk to the Dev Bioindex deliberately, as changes are made there, and then all the Dev Bioindex's data is copied to make a new data snapshot for the prod instance.  The Prod Bioindex is never updated directly.

Alter the paths in this configuration for your specific machine, and append it to your `~/.ssh/config`:
```
Host aggregator
    HostName 3.218.217.187
    Port 22
    User ec2-user
    IdentityFile /path/to/your/copy/of/GenomeStoreREST.pem
Host bioindex
    HostName ec2-18-215-38-136.compute-1.amazonaws.com 
    Port 22
    User ec2-user
    IdentityFile /path/to/your/copy/of/GenomeStoreREST.pem
```

# Future Work
This is a classic case for Loamstream automation, as LS provides a lot of things that are missing from these scripts that is either re-invented here in a less-robust fashion or missing, like being able to resume midway.
