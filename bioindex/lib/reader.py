import os
import random
import subprocess
import time

import boto3
import botocore.exceptions
import dataclasses
import itertools
import logging
import orjson

from .auth import verify_record
from .s3 import read_lined_object


# a bgzip read fails transiently often enough to be worth retrying: an S3 503
# SlowDown, a dropped connection. Exponential backoff with full jitter, tunable
# from the environment so a noisy deployment can be adjusted without a release.
BGZIP_MAX_RETRIES = int(os.environ.get('BIOINDEX_BGZIP_MAX_RETRIES', '3'))
BGZIP_BACKOFF_BASE_S = float(os.environ.get('BIOINDEX_BGZIP_BACKOFF_BASE_S', '0.2'))
BGZIP_BACKOFF_CAP_S = float(os.environ.get('BIOINDEX_BGZIP_BACKOFF_CAP_S', '5.0'))

# one session per process; boto3 caches the credentials and refreshes them
_session = boto3.Session()


def _bgzip_env():
    """
    The environment for a bgzip subprocess, carrying whatever credentials
    boto3 can resolve. htslib reads AWS credentials from the environment or
    ~/.aws/credentials and never from the container metadata endpoint a task
    role is delivered through, so it cannot find them on its own.
    """
    env = os.environ.copy()
    credentials = _session.get_credentials()

    if credentials is not None:
        frozen = credentials.get_frozen_credentials()
        env['AWS_ACCESS_KEY_ID'] = frozen.access_key
        env['AWS_SECRET_ACCESS_KEY'] = frozen.secret_key

        # all three come from one source or none do; leaving an inherited
        # token next to keys resolved elsewhere fails the signature
        env.pop('AWS_SESSION_TOKEN', None)
        if frozen.token:
            env['AWS_SESSION_TOKEN'] = frozen.token

    return env


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
        self.matched_bytes = 0
        self.filtered_count = 0
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

        # start reading the records on-demand; _readall applies record_filter
        # inline as it counts, so there is no second pass over the records
        self.record_filter = record_filter
        self.records = self._readall()

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

            if source.bounded and seek_length <= 0:
                # already at/past end of this bounded source (e.g. byte_offset == source.length).
                # Guard applies only to bounded sources: for unbounded sources (e.g. /all),
                # source.end is the compressed byte size — not comparable to the uncompressed
                # seek_start — so the guard must not fire even when seek_start > source.end.
                continue

            try:
                compression_on = self.index.compressed
                if compression_on:
                    yield from self._read_compressed(source)

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
                        yield from self._consume(line, len(line.encode('utf-8')) + 1)

            # handle database out of sync with S3
            except botocore.exceptions.ClientError:
                logging.error('Failed to read key %s; some records missing', source.key)
            except FileNotFoundError:
                logging.error('Failed to read key %s; some records missing', source.key)

        # outer source-loop completed normally — every source fully consumed
        self._exhausted = True

    def _read_compressed(self, source):
        """
        Yield the records of one bgzip source, retrying a failed read.

        Every attempt recomputes the seek from the current byte offset, so a
        retry picks up where the broken stream stopped and no record is
        yielded twice.
        """
        suffix = '' if source.key.endswith('.gz') else '.gz'
        s3_url = f's3://{self.config.s3_bucket}/{source.key}{suffix}'

        for attempt in range(BGZIP_MAX_RETRIES + 1):
            seek_start = source.start + self._source_byte_offset

            # bgzip's -s counts uncompressed bytes, so only a bounded source
            # can be limited with it: an unbounded source's end is the
            # COMPRESSED size, which would truncate the output mid-record,
            # and subtracting from it goes negative once the uncompressed
            # offset passes it, dropping the rest of the stream. An unbounded
            # read instead ends at real EOF, when bgzip exits 0.
            if source.bounded:
                seek_length = source.end - seek_start
                if seek_length <= 0:
                    return
                command = ['bgzip', '-b', f'{seek_start}', '-s', f'{seek_length}', s3_url]
            else:
                command = ['bgzip', '-b', f'{seek_start}', s3_url]

            try:
                yield from self._run_bgzip(command)
                return
            except subprocess.CalledProcessError as e:
                stderr = (e.output or '').strip()
                if attempt == BGZIP_MAX_RETRIES:
                    logging.error('bgzip failed on %s at byte %d after %d attempts, giving up: %s',
                                  source.key, self._source_byte_offset, attempt + 1, stderr)
                    raise

                delay = random.uniform(0, min(BGZIP_BACKOFF_CAP_S, BGZIP_BACKOFF_BASE_S * 2 ** attempt))
                logging.warning('bgzip failed on %s at byte %d (attempt %d of %d), retrying in %.3fs: %s',
                                source.key, self._source_byte_offset, attempt + 1,
                                BGZIP_MAX_RETRIES + 1, delay, stderr)
                time.sleep(delay)

    def _run_bgzip(self, command):
        """
        Yield the records of a single bgzip invocation.

        Read in binary: line iteration then splits only on b'\\n' with no
        universal-newline translation, so len(line) is the true uncompressed
        byte count even for a CRLF-terminated or multi-byte record. Decoded,
        '\\r\\n' would collapse to '\\n' and the count would be characters,
        under-counting the line and landing the next resume's -b on a
        stranded newline that orjson then rejects as empty input.
        """
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              env=_bgzip_env()) as proc:
            for line in proc.stdout:
                yield from self._consume(line, len(line))

            proc.wait()
            if proc.returncode != 0:
                raise subprocess.CalledProcessError(
                    proc.returncode, command,
                    output=proc.stderr.read().decode(errors='replace'),
                )

    def _consume(self, line, line_bytes):
        """
        Account for one raw line and yield its record, unless the record is
        restricted or the filter rejects it.
        """
        self.bytes_read += line_bytes
        self._source_byte_offset += line_bytes

        # orjson.loads takes either bytes or str
        record = orjson.loads(line)

        if not verify_record(record, self.restricted):
            self.restricted_count += 1
            return

        if self.record_filter is None or self.record_filter(record):
            self.count += 1
            self.matched_bytes += line_bytes
            yield record
        else:
            self.filtered_count += 1

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
    def matched_bytes(self):
        """
        Total bytes of the records actually returned.
        """
        return sum(r.matched_bytes for r in self.readers)

    @property
    def restricted_count(self):
        """
        Total number of restricted records read.
        """
        return sum(r.restricted_count for r in self.readers)

    @property
    def filtered_count(self):
        """
        Total number of records read and then dropped by the filter.
        """
        return sum(r.filtered_count for r in self.readers)

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
