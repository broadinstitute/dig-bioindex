import msgpack
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
        indicating whether the table already existed (True). The schema should be
        either 'json' (the default) or a registered CSV dialect name.
        """
        table_path = f'table/{table.path}'

        # ensure the table isn't already indexed
        table_id = self._r.get(table_path)
        already_exists = table_id is not None

        # create a new table id
        if already_exists:
            table_id = int(table_id)
        else:
            table_id = self._r.incr('table_id')

            # index the table name to its value (can ensure unique tables)
            self._r.set(table_path, table_id)

        # set - or update - the table values
        self._r.hset(f'table:{table_id}', 'path', table.path)
        self._r.hset(f'table:{table_id}', 'key', table.key)
        self._r.hset(f'table:{table_id}', 'locus', table.locus)
        self._r.hset(f'table:{table_id}', 'dialect', table.dialect)
        self._r.hset(f'table:{table_id}', 'fieldnames', msgpack.dumps(table.fieldnames))

        return table_id, already_exists

    def scan_tables(self, prefix=None):
        """
        Returns a generator of table IDs.
        """
        for key in self._r.scan_iter(f'table:{prefix if prefix else ""}*'):
            yield int(key.split(b':')[1])

    def get_table(self, table_id):
        """
        Returns a map of the table entry for the given id.
        """
        table = self._r.hgetall(f'table:{table_id}')
        if not table:
            raise KeyError(f'Table {table_id} does not exist')

        # if this table has field names (CSV), unpack them
        cols = msgpack.loads(table.get(b'fieldnames'))
        if cols:
            cols = list(map(lambda s: s.decode('utf-8'), cols))

        return Table(
            path=table[b'path'].decode('utf-8'),
            key=table[b'key'].decode('utf-8'),
            locus=table[b'locus'].decode('utf-8'),
            dialect=table[b'dialect'].decode('utf-8'),
            fieldnames=cols,
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
        path = self._r.hget(f'table:{table_id}', 'path').decode('utf-8')

        # delete the table key and path
        self._r.delete(f'table/{path}')
        self._r.delete(f'table:{table_id}')

    def delete_records(self, table_id):
        """
        Delete all records associated with a given table. The table remains in
        the database as a valid ID which can be used. Useful for updating all
        the records of a table.
        """
        table = self.get_table(table_id)

        # instead of decoding every record, use the beginning prefix of a fake,
        # encoded record to match against
        match = b'\x93' + msgpack.dumps(table_id) + b'*'

        # collect the list of all records to delete
        with self._r.pipeline() as pipe:
            pipe.multi()

            # find all records associated with table in the table's key space
            for k in (f'{table.key}:{c}' for c in chromosomes()):
                if not self._r.exists(k):
                    continue

                if self._r.type(k) == b'zset':
                    pipe.zrem(k, *self._r.zscan_iter(k, match=match, count=10000))
                else:
                    for bucket in self._r.smembers(k):
                        if self._r.type(bucket) == b'set':
                            pipe.srem(bucket, *self._r.sscan_iter(bucket, match=match, count=10000))

            # do it
            pipe.execute()

    def get_table_keys(self):
        """
        Query all tables for a unique list of indexed key spaces.
        """
        keys = set()

        for table_id in self.scan_tables():
            keys.add(self.get_table(table_id).key)

        return list(keys)

    def insert_records(self, base_key, records):
        """
        Use the key type of the records map to determine whether to insert
        as SNPs or regions.
        """
        with self._r.pipeline() as pipe:
            pipe.multi()

            # add each record
            for locus, record in records.items():
                value = record.pack()
                base_chr = f'{base_key}:{locus.chromosome}'

                # SNP records are stored as an ordered set
                if isinstance(locus, SNPLocus):
                    pipe.zadd(base_chr, {value: locus.position})

                # Regions are stored as sets across fixed-sized buckets
                if isinstance(locus, RegionLocus):
                    for bucket in range(locus.start // 20000, locus.stop // 20000 + 1):
                        bucket_key = f'{base_chr}:{bucket}'

                        # each chromosome knows which buckets it contains (fast scanning)
                        pipe.sadd(base_chr, bucket_key)
                        pipe.sadd(bucket_key, value)

            # insert all values atomically
            pipe.execute()

    def count_records(self, key, chromosome, start, stop):
        """
        Count the number of records overlapped by a given locus. This count
        may not be 100% accurate for region records, because they may overlap
        a bucket but not the locus of the query. Without actually fetching
        the records it isn't possible to know if the record overlaps. At worst,
        though, this will return false positives (more records), but no false
        negatives (fewer).
        """
        chr_key = f'{key}:{chromosome}'

        # does the chromosome maps to an ordered set (SNP records)?
        if self._r.type(chr_key) == b'zset':
            n = self._r.zcount(chr_key, start, stop)
        else:
            n = 0

            # query records across the bucket range
            for i in range(start // 20000, stop // 20000 + 1):
                n += self._r.scard(f'{chr_key}:{i}')

        return n

    def query_records(self, key, chromosome, start, stop):
        """
        Queries all the records overlapped by a given locus. Uses the type of the key
        to determine the query type. Returns a map of table_id -> [(offset, length)].
        """
        chr_key = chr_key = f'{key}:{chromosome}'
        results = dict()

        # does the chromosome maps to an ordered set (SNP records)?
        if self._r.type(chr_key) == b'zset':
            query_results = self._r.zrangebyscore(chr_key, start, stop)
        else:
            query_results = set()

            # query records across the bucket range
            for i in range(start // 20000, stop // 20000 + 1):
                members = self._r.smembers(f'{chr_key}:{i}')

                # each record should only exist once
                query_results.update(members)

        # unpack records, extract tables
        for r in query_results:
            record = msgpack.loads(r)

            # NOTE: record may overlap bucket but not locus!!

            results.setdefault(record[0], list()). \
                append(record[1:])

        return results
