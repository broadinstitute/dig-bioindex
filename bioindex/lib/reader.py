import subprocess

import botocore.exceptions
import dataclasses
import itertools
import logging
import orjson

from .auth import verify_record
from .s3 import read_lined_object
# from . import config

# CONFIG = config.Config()


class RecordSource:
    def __init__(self, key, start, end, record_filter):
        self.key = key
        self.start = start
        self.end = end
        self.record_filter = record_filter


    @staticmethod
    def from_s3_object(s3_obj, record_filter):
        """
        Create a RecordSource from an S3 object listing.
        """
        return RecordSource(
            key=s3_obj['Key'],
            start=0,
            end=s3_obj['Size'],
            record_filter=record_filter
        )

    @property
    def length(self):
        """
        Returns the number of bytes to read total.
        """
        return self.end - self.start


class RecordReader:
    """
    A RecordReader is an iterator that reads all the JSON-lines (records)
    from a list of RecordSource objects for a given S3 bucket.
    """

    def __init__(self, config, sources, index, restricted=None):
        """
        Initialize the RecordReader with a list of RecordSource objects.
        """
        self.config = config
        self.sources = sources
        self.restricted = restricted
        self.index = index
        self.bytes_total = 0
        self.bytes_read = 0
        self.count = 0
        self.restricted_count = 0
        self.limit = None

        # sum the total number of bytes to read
        for source in sources:
            self.bytes_total += source.length

        # start reading the records on-demand
        self.records = self._readall()


    def _readall(self):
        """
        A generator that reads each of the records from S3 for the sources.
        """
        for source in self.sources:

            # This is here to handle a particularly bad condition: when the
            # byte offsets are mucked up and this would cause the reader to
            # read everything from the source file (potentially GB of data)
            # which will have time and bandwidth costs.

            if source.end <= source.start:
                logging.warning('Bad index record: end offset <= start; skipping...')
                continue

            try:
                compression_on = self.index.compressed
                if compression_on:
                    command = ['bgzip', '-b', f"{source.start}", '-s', f"{source.end - source.start}",
                               f"s3://{self.config.s3_bucket}/{source.key}{'' if source.key.endswith('.gz') else '.gz'}"]
                    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1) as proc:
                        for line in proc.stdout:
                            self.bytes_read += len(line) + 1  # eol character

                            # parse the record
                            record = orjson.loads(line)

                            # Check for restrictions and filters, then yield records
                            if not verify_record(record, self.restricted):
                                self.restricted_count += 1
                                continue

                            if source.record_filter is None or source.record_filter(record):
                                self.count += 1
                                yield record

                        proc.wait()
                        if proc.returncode != 0:
                            stderr = proc.stderr.read()
                            raise subprocess.CalledProcessError(proc.returncode, command, output=stderr)

                else:
                    content = read_lined_object(self.config.s3_bucket, source.key, offset=source.start,
                                          length=source.end - source.start)

                    # handle a bad case where the content failed to be read
                    if content is None:
                        raise FileNotFoundError(source.key)

                    for line in content:
                        self.bytes_read += len(line) + 1  # eol character

                        # parse the record
                        record = orjson.loads(line)

                        # are there any restrictions on this record?
                        if not verify_record(record, self.restricted):
                            self.restricted_count += 1
                            continue

                        # optionally filter; and tally filtered records
                        if source.record_filter is None or source.record_filter(record):
                            self.count += 1
                            yield record

            # handle database out of sync with S3
            except botocore.exceptions.ClientError:
                logging.error('Failed to read key %s; some records missing', source.key)
            except FileNotFoundError:
                logging.error('Failed to read key %s; some records missing', source.key)

    @property
    def at_end(self):
        """
        True if all records have been read.
        """
        if self.limit and self.count >= self.limit:
            return True

        return self.bytes_read >= self.bytes_total

    def set_limit(self, limit):
        """
        Apply a limit to the number of records that will be read.
        """
        self.limit = limit

        # update the iterator so it stops once the limit is reached
        self.records = itertools.takewhile(lambda _: self.count <= self.limit, self.records)
