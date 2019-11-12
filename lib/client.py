import msgpack
import redis

from .locus import *
from .table import *


class Client:
    """
    Create a new Redis client.
    """

    def __init__(self, **kwargs):
        """
        Connect to Redis server.
        """
        self._r = redis.Redis(**kwargs)

    def __enter__(self):
        """
        Do scope initialization here.
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_trace):
        """
        Close the connection and commit any changes.
        """
        if not exc_value:
            self._r.save()

    def register_table(self, table):
        """
        Create a new table key if it doesn't exist yet. Returns the ID and a flag
        indicating whether the table already existed (True).
        """
        table_uri = 'table.uri:%s/%s' % (table.bucket, table.path)

        # ensure the table isn't already indexed
        table_id = self._r.get(table_uri)
        if table_id:
            return table_id, True

        # get the next table id
        table_id = self._r.incr('table_id')

        # define the table with the given id
        self._r.hset('table:%d' % table_id, 'bucket', table.bucket)
        self._r.hset('table:%d' % table_id, 'path', table.path)
        self._r.hset('table:%d' % table_id, 'key', table.key)
        self._r.hset('table:%d' % table_id, 'locus', table.locus)

        # index the table name to its value (can ensure unique tables)
        self._r.set(table_uri, table_id)

        return table_id, False

    def scan_tables(self):
        """
        Returns a generator of table IDs.
        """
        for key in self._r.scan_iter("table:*"):
            yield int(key.split(b':')[1])

    def get_table(self, table_id):
        """
        Returns a map of the table entry for the given id.
        """
        table = self._r.hgetall('table:%d' % table_id)

        return Table(
            bucket=table[b'bucket'].decode('utf-8'),
            path=table[b'path'].decode('utf-8'),
            key=table[b'key'].decode('utf-8'),
            locus=table[b'locus'].decode('utf-8'),
        )

    def delete_table(self, table_id):
        """
        Remove a table and ALL records that reference it.
        """
        pass

    def insert_records(self, base_key, records):
        """
        Use the key type of the records map to determine whether to insert
        as SNPs or regions.
        """
        with self._r.pipeline() as pipe:
            pipe.multi()

            # add each record
            for locus, record in records.items():
                value = msgpack.packb(record)

                # SNP records are stored as an ordered set
                if isinstance(locus, SNPLocus):
                    key = '%s:%s' % (base_key, locus.chromosome)
                    pipe.zadd(key, {value: locus.position})

                # Regions are stored as sets across fixed-sized buckets
                if isinstance(locus, RegionLocus):
                    for bucket in range(locus.start // 20000, locus.stop // 20000 + 1):
                        key = '%s:%s:%d' % (base_key, locus.chromosome, bucket)
                        pipe.sadd(key, value)

            # insert all values atomically
            pipe.execute()

    def count_records(self, key, chromosome, start, stop):
        """
        Count the number of records overlapped by a given locus.
        """
        chr_key = '%s:%s' % (key, chromosome)

        # does the chromosome maps to an ordered set (SNP records)?
        if self._r.type(chr_key) == b'zset':
            n = self._r.zcount(chr_key, start, stop)
        else:
            n = 0

            # query records across the bucket range
            for i in range(start // 20000, stop // 20000 + 1):
                key = '%s:%d' % (chr_key, i)
                n += self._r.scard(key)

        return n

    def query_records(self, key, chromosome, start, stop):
        """
        Queries all the records overlapped by a given locus. Uses the type of the key
        to determine the query type. Returns a map of table_id -> [(offset, length)].
        """
        chr_key = '%s:%s' % (key, chromosome)
        results = dict()

        # does the chromosome maps to an ordered set (SNP records)?
        if self._r.type(chr_key) == b'zset':
            query_results = self._r.zrangebyscore(chr_key, start, stop)
        else:
            query_results = set()

            # query records across the bucket range
            for i in range(start // 20000, stop // 20000 + 1):
                key = '%s:%d' % (chr_key, i)
                members = self._r.smembers(key)

                # each record should only exist once
                query_results.update(members)

        # unpack records, extract tables
        for r in query_results:
            record = msgpack.loads(r)

            # NOTE: record may overlap bucket but not locus!!

            results.setdefault(record[0], list()). \
                append((record[1], record[2]))

        return results
