import msgpack
import os
import redis

from .locus import *
from .table import *


class Client:
    """
    Create a new Redis client.
    """

    def __init__(self, readonly=False, **kwargs):
        """
        Connect to Redis server.
        """
        host = os.getenv('REDIS_HOST', 'localhost')
        port = os.getenv('REDIS_PORT', 6379)

        # connect to redis server
        self._r = redis.Redis(host=host, port=int(port), **kwargs)
        self._readonly = readonly

    def __enter__(self):
        """
        Do scope initialization here.
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_trace):
        """
        Close the connection and commit any changes.
        """
        if not self._readonly and not exc_value:
            self._r.save()

    def register_table(self, table):
        """
        Create a new table key if it doesn't exist yet. Returns the ID and a flag
        indicating whether the table already existed (True).
        """
        table_path = 'table/%s' % table.path

        # ensure the table isn't already indexed
        table_id = self._r.get(table_path)
        if table_id:
            return int(table_id), True

        # get the next table id
        table_id = self._r.incr('table_id')

        # define the table with the given id
        self._r.hset('table:%d' % table_id, 'path', table.path)
        self._r.hset('table:%d' % table_id, 'key', table.key)
        self._r.hset('table:%d' % table_id, 'locus', table.locus)

        # index the table name to its value (can ensure unique tables)
        self._r.set(table_path, table_id)

        return table_id, False

    def scan_tables(self, prefix=None):
        """
        Returns a generator of table IDs.
        """
        for key in self._r.scan_iter('table:%s*' % prefix if prefix else ''):
            yield int(key.split(b':')[1])

    def get_table(self, table_id):
        """
        Returns a map of the table entry for the given id.
        """
        table = self._r.hgetall('table:%d' % table_id)
        if not table:
            raise KeyError('Table %d does not exist' % table_id)

        return Table(
            path=table[b'path'].decode('utf-8'),
            key=table[b'key'].decode('utf-8'),
            locus=table[b'locus'].decode('utf-8'),
        )

    def delete_table(self, table_id):
        """
        Removes all records associated with a table (via delete_records) and
        then removes the table as well. The table ID will no longer be valid
        after this call and will need to be re-registered with a new ID if
        it needs to be added back.
        """
        self.delete_records(table_id)

        # lookup the path to delete the reverse lookup key
        path = self._r.hget('table:%d' % table_id, 'path').decode('utf-8')

        # delete the table key and path
        self._r.delete('table/%s' % path)
        self._r.delete('table:%d' % table_id)

    def delete_records(self, table_id):
        """
        Delete all records associated with a given table. The table remains in
        the database as a valid ID which can be used. Useful for updating all
        the records of a table.
        """
        table = self.get_table(table_id)

        # extracts records from this table
        def filter_records(records):
            return filter(lambda r: msgpack.loads(r)[0] == table_id, records)

        # scan all records in the key space for the table
        with self._r.pipeline() as pipe:
            pipe.multi()

            for k in self._r.scan_iter('%s:*' % table.key):
                if self._r.type(k) == b'zset':
                    pipe.zrem(k, *filter_records(self._r.zrange(k, 0, -1)))
                else:
                    pipe.srem(k, *filter_records(self._r.smembers(k)))

            # do it
            pipe.execute()

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
