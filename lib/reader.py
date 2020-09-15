import botocore.exceptions
import dataclasses
import itertools
import logging
import orjson

import lib.auth
import lib.locus
import lib.s3
import lib.schema


@dataclasses.dataclass(frozen=True)
class RecordSource:
    """
    A RecordSource is a portion of an S3 object that contains JSON-
    lines records.
    """
    key: str
    start: int
    end: int

    @staticmethod
    def from_s3_object(s3_obj):
        """
        Create a RecordSource from an S3 object listing.
        """
        return RecordSource(
            key=s3_obj['Key'],
            start=0,
            end=s3_obj['Size'],
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

    def __init__(self, bucket, sources, record_filter=None, restricted=None):
        """
        Initialize the RecordReader with a list of RecordSource objects.
        """
        self.bucket = bucket
        self.sources = sources
        self.restricted = restricted
        self.bytes_total = 0
        self.bytes_read = 0
        self.count = 0
        self.restricted_count = 0
        self.limit = None

        # sum the total number of bytes to read
        for source in sources:
            self.bytes_total += source.length

        # start reading the records on-demand
        self.record_filter = record_filter
        self.records = self._readall()

        # if there's a filter, apply it now
        if record_filter is not None:
            self.records = filter(record_filter, self.records)

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
                content = lib.s3.read_object(
                    self.bucket,
                    source.key,
                    offset=source.start,
                    length=source.end - source.start,
                )

                for line in content.iter_lines():
                    self.bytes_read += len(line) + 1  # eol character

                    # parse the record
                    record = orjson.loads(line)

                    # are there any restrictions on this record?
                    if not lib.auth.verify_record(record, self.restricted):
                        self.restricted_count += 1
                        continue

                    # optionally filter; and tally filtered records
                    if self.record_filter is None or self.record_filter(record):
                        self.count += 1
                        yield record

            # handle database out of sync with S3
            except botocore.exceptions.ClientError:
                logging.error('Failed to read table %s; some records missing', source.key)

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
