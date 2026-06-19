import subprocess

import botocore.exceptions
import dataclasses
import itertools
import logging
import orjson

from .auth import verify_record
from .s3 import read_lined_object


@dataclasses.dataclass(frozen=True)
class RecordSource:
    """
    A RecordSource is a portion of an S3 object that contains JSON-
    lines records.

    bounded: whether ``start`` and ``end`` are real uncompressed-byte
        offsets (True, for SQL-derived sources) or compressed-byte hints
        (False, for sources from S3 listings — used by /all). For
        unbounded sources on compressed indexes the reader must NOT pass
        ``end`` to bgzip's ``-s`` flag (which is uncompressed bytes),
        because doing so would truncate output at ~compressed_size of
        uncompressed data and cut off mid-record.
    """
    key: str
    start: int
    end: int
    bounded: bool = True

    @staticmethod
    def from_s3_object(s3_obj):
        """
        Create a RecordSource from an S3 object listing. The ``end``
        here is the S3 object's compressed-byte size, which is only an
        approximate progress hint — bounded=False signals to the reader
        that no uncompressed boundary is known.
        """
        return RecordSource(
            key=s3_obj['Key'],
            start=0,
            end=s3_obj['Size'],
            bounded=False,
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

    def __init__(self, config, sources, index, record_filter=None, restricted=None,
                 start_source_index=0, start_byte_offset=0):
        """
        Initialize the RecordReader with a list of RecordSource objects.

        start_source_index: resume reading from this source index (inclusive)
        start_byte_offset: number of bytes already consumed within
            start_source_index (measured from source.start). On resume, the
            reader opens the S3 range at source.start + start_byte_offset
            instead of source.start, so previously-returned bytes are not
            re-downloaded.
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
        self._source_index = start_source_index
        self._source_byte_offset = 0
        self._start_source_index = start_source_index
        self._start_byte_offset = start_byte_offset
        # Set True only after the outer source loop completes naturally
        # (every source fully consumed). Used by at_end to distinguish
        # "iterator exhausted" from "broke at byte limit".
        self._exhausted = False

        # only count bytes from the resume point onward; on the resume source,
        # discount the bytes already consumed by previous pages so bytes_read
        # (which only counts bytes read by THIS reader) can match bytes_total
        # at end-of-stream. Unbounded sources contribute their (compressed)
        # length as an approximate progress hint; at_end no longer relies on
        # bytes_read >= bytes_total for them.
        for j, source in enumerate(sources[start_source_index:], start=start_source_index):
            length = source.length
            if j == start_source_index and source.bounded:
                length = max(0, length - start_byte_offset)
            self.bytes_total += length

        # start reading the records on-demand
        self.record_filter = record_filter
        self.records = self._readall()

        # if there's a filter, apply it now
        if record_filter is not None:
            self.records = filter(record_filter, self.records)

    def _readall(self):
        """
        A generator that reads each of the records from S3 for the sources.

        On resume, the reader opens source[start_source_index] at
        source.start + start_byte_offset, so previously-returned bytes are
        not re-downloaded. _source_byte_offset tracks the cumulative bytes
        consumed within the current source, measured from source.start, and
        can be used directly as start_byte_offset on the next resume.
        """
        for i, source in enumerate(self.sources):
            # skip sources before the resume point
            if i < self._start_source_index:
                continue

            self._source_index = i

            # Cumulative byte offset within source i, measured from source.start.
            # On the resume source, start from the byte offset so the value can
            # be used directly as start_byte_offset for the next resume call.
            if i == self._start_source_index:
                self._source_byte_offset = self._start_byte_offset
                seek_start = source.start + self._start_byte_offset
            else:
                self._source_byte_offset = 0
                seek_start = source.start

            seek_length = source.end - seek_start

            # This is here to handle a particularly bad condition: when the
            # byte offsets are mucked up and this would cause the reader to
            # read everything from the source file (potentially GB of data)
            # which will have time and bandwidth costs.

            if source.end <= source.start:
                logging.warning('Bad index record: end offset <= start; skipping...')
                continue

            if seek_length <= 0:
                # already at/past end of this source (e.g. byte_offset == source.length)
                continue

            try:
                compression_on = self.index.compressed
                if compression_on:
                    # For bounded sources (SQL-derived), pass -s to limit bgzip
                    # to the known uncompressed range. For unbounded sources
                    # (from S3 listing, /all path), source.end is the COMPRESSED
                    # size — passing it as -s would truncate uncompressed output
                    # at ~compressed_size bytes (mid-record). Omit -s so bgzip
                    # reads to actual EOF.
                    s3_url = f"s3://{self.config.s3_bucket}/{source.key}{'' if source.key.endswith('.gz') else '.gz'}"
                    if source.bounded:
                        command = ['bgzip', '-b', f"{seek_start}", '-s', f"{seek_length}", s3_url]
                    else:
                        command = ['bgzip', '-b', f"{seek_start}", s3_url]
                    # Read bgzip output in BINARY (no text=True). Binary line
                    # iteration splits only on b'\n' with NO universal-newline
                    # translation, so len(line) is the true uncompressed byte
                    # count (including the newline) even for CRLF-terminated or
                    # multi-byte records. text=True would collapse '\r\n' -> '\n'
                    # and count characters, under-counting such a line by a byte
                    # and landing the next resume's bgzip -b on a stranded
                    # newline (orjson then fails with "input data is empty").
                    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
                        for line in proc.stdout:
                            line_bytes = len(line)
                            self.bytes_read += line_bytes
                            self._source_byte_offset += line_bytes

                            # parse the record (orjson.loads accepts bytes)
                            record = orjson.loads(line)

                            # Check for restrictions and filters, then yield records
                            if not verify_record(record, self.restricted):
                                self.restricted_count += 1
                                continue

                            if self.record_filter is None or self.record_filter(record):
                                self.count += 1
                                yield record

                        proc.wait()
                        if proc.returncode != 0:
                            stderr = proc.stderr.read().decode(errors="replace")
                            raise subprocess.CalledProcessError(proc.returncode, command, output=stderr)

                else:
                    content = read_lined_object(self.config.s3_bucket, source.key,
                                                offset=seek_start, length=seek_length)

                    # handle a bad case where the content failed to be read
                    if content is None:
                        raise FileNotFoundError(source.key)

                    for line in content:
                        # read_lined_object yields decoded, newline-stripped str.
                        # Count true UTF-8 bytes (+1 for the stripped \n) so this
                        # matches the byte offsets stored in __Keys, which index.py
                        # writes as len(line.encode('utf-8')) + 1. Plain len(line)
                        # under-counts multi-byte chars and desyncs resume.
                        line_bytes = len(line.encode('utf-8')) + 1  # eol byte
                        self.bytes_read += line_bytes
                        self._source_byte_offset += line_bytes

                        # parse the record
                        record = orjson.loads(line)

                        # are there any restrictions on this record?
                        if not verify_record(record, self.restricted):
                            self.restricted_count += 1
                            continue

                        # optionally filter; and tally filtered records
                        if self.record_filter is None or self.record_filter(record):
                            self.count += 1
                            yield record

            # handle database out of sync with S3
            except botocore.exceptions.ClientError:
                logging.error('Failed to read key %s; some records missing', source.key)
            except FileNotFoundError:
                logging.error('Failed to read key %s; some records missing', source.key)

        # outer source-loop completed normally — every source fully consumed
        self._exhausted = True

    @property
    def at_end(self):
        """
        True if all records have been read.
        """
        if self.limit and self.count >= self.limit:
            return True

        if self._exhausted:
            return True

        # bytes_read >= bytes_total is a useful fallback for bounded sources
        # (where bytes_total is an exact upper bound on uncompressed bytes
        # delivered). For unbounded /all sources bytes_total is the SUM of
        # compressed sizes, which is in different units than bytes_read, so
        # this check could fire spuriously. Guard with all(bounded).
        if self.bytes_total > 0 and all(s.bounded for s in self.sources):
            return self.bytes_read >= self.bytes_total

        return False

    def set_limit(self, limit):
        """
        Apply a limit to the number of records that will be read.
        """
        self.limit = limit

        # update the iterator so it stops once the limit is reached
        self.records = itertools.takewhile(lambda _: self.count <= self.limit, self.records)


class MultiRecordReader:
    """
    A RecordReader that's the aggregate of several readers chained
    together into a single reader.
    """

    def __init__(self, readers):
        """
        Initialize with the several readers.
        """
        self.readers = readers
        self.records = itertools.chain(*(r.records for r in readers))
        self.limit = None

    @property
    def buckets(self):
        """
        All buckets.
        """
        return [r.bucket for r in self.readers]

    @property
    def sources(self):
        """
        All sources.
        """
        return [s for s in r.sources for r in self.readers]

    @property
    def bytes_total(self):
        """
        Total bytes to read.
        """
        return sum(r.bytes_total for r in self.readers)

    @property
    def bytes_read(self):
        """
        Total bytes read.
        """
        return sum(r.bytes_read for r in self.readers)

    @property
    def count(self):
        """
        Total number of records read.
        """
        return sum(r.count for r in self.readers)

    @property
    def restricted_count(self):
        """
        Total number of restricted records read.
        """
        return sum(r.restricted_count for r in self.readers)

    @property
    def at_end(self):
        """
        True if all records have been read.
        """
        return all(r.at_end for r in self.readers)

    def set_limit(self, limit):
        """
        Apply a limit to the number of records that will be read.
        """
        for r in self.readers:
            r.set_limit(limit)
