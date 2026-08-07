import concurrent.futures
import csv
import datetime
import functools
import logging
import orjson
import os
import os.path
import rich.progress
import sqlalchemy
import tempfile
import threading
import time

from sqlalchemy import text

from .aws import invoke_lambda, start_and_wait_for_group_indexer_job, start_and_wait_for_indexer_job
from .s3 import list_objects, read_lined_object, relative_key
from .schema import Schema
from .utils import cap_case_str


def _chunk_objects(objects, max_files, max_bytes):
    """Partition S3 objects into groups bounded by file count and total bytes (count wins ties)."""
    groups, cur, cur_bytes = [], [], 0
    for o in objects:
        size = o.get('Size', 0)
        if cur and (len(cur) >= max_files or cur_bytes + size > max_bytes):
            groups.append(cur)
            cur, cur_bytes = [], 0
        cur.append(o)
        cur_bytes += size
    if cur:
        groups.append(cur)
    return groups


def _key_is_current(db_keys, obj):
    """True if obj's key is already indexed at its current S3 version (ETag)."""
    k = obj['Key']
    return k in db_keys and db_keys[k]['version'] == obj['ETag'].strip('"')[:32]


def list_index_objects(bucket, s3_path, prefer_compressed):
    """Deterministic ordered object listing for an index prefix (shared by parent and worker).

    A prefix holds both extensions only mid-compression: the .gz are written first and the
    .json originals are deleted last, so that a failed verify can still roll back. With
    prefer_compressed the .gz win and the pending-delete originals are ignored; without it
    the ambiguity is an error.
    """
    json_objects = list(list_objects(bucket, s3_path, only='*.json'))
    gz_objects = list(list_objects(bucket, s3_path, only='*.json.gz'))
    if json_objects and gz_objects:
        if prefer_compressed:
            logging.info('%s has both .json and .json.gz; preferring .json.gz (%d) and ignoring %d '
                         'pending-delete .json original(s).', s3_path, len(gz_objects), len(json_objects))
            json_objects = []
        else:
            raise ValueError(
                f'{s3_path} has both .json and .json.gz; an index must be all one or the other.'
            )
    return json_objects + gz_objects

# Both mean another builder holds the lock and the statement is worth
# retrying; 1205's own message says "try restarting transaction".
MYSQL_ER_LOCK_DEADLOCK = 1213
MYSQL_ER_LOCK_WAIT_TIMEOUT = 1205

RETRYABLE_MYSQL_ERRORS = frozenset({MYSQL_ER_LOCK_DEADLOCK, MYSQL_ER_LOCK_WAIT_TIMEOUT})


def _is_retryable(error):
    """
    True if MySQL rejected the statement over lock contention, which passes.
    Anything else - a missing table, a bad value - fails the same way five
    times in a row, so it is raised rather than retried.

    Not `error.code`: that is SQLAlchemy's own documentation code for the
    exception class - the string 'e3q8' for OperationalError - and comparing
    it to a MySQL error number is never true. The database's number is on
    the DBAPI exception underneath.
    """
    orig = getattr(error, 'orig', None)
    args = getattr(orig, 'args', None)

    return bool(args) and args[0] in RETRYABLE_MYSQL_ERRORS


# `__Indexes` is unique on (name, arity), but arity is derived from the schema
# string rather than stored. Deriving it the same way the constraint does keeps
# a lookup from ever disagreeing with it about which row is which.
ARITY_SQL = "LENGTH(`schema`) - LENGTH(REPLACE(`schema`, ',', '')) + 1"

# MySQL's zero date. `built` is nullable, but rows written while `create` set it
# to 0 hold this instead of NULL, and it arrives from the driver as a string -
# neither None nor falsy - so anything asking "was this ever built?" has to name
# it explicitly or get back yes.
ZERO_DATETIME = '0000-00-00 00:00:00'


def _is_built(built):
    """
    True if `built` records a time an index was actually built.
    """
    return built is not None and str(built) != ZERO_DATETIME


class Index:
    """
    An index definition that can be built or queried.
    """

    def __init__(self, name, table_name, s3_prefix, schema_string, built_date, compressed):
        """
        Initialize the index with everything needed to build keys and query.
        """
        self.schema = Schema(schema_string)
        self.table = self.schema.table_def(table_name, sqlalchemy.MetaData())
        self.name = name
        self.built = built_date
        self.s3_prefix = s3_prefix
        self.compressed = compressed

    @property
    def is_built(self):
        """
        True if this index has actually been built.

        Not the same as a truthy `built`: see ZERO_DATETIME.
        """
        return _is_built(self.built)

    @staticmethod
    def set_compressed(engine, name, prefix, compressed):
        with engine.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    'UPDATE `__Indexes` SET compressed = :compressed '
                    'WHERE `name` = :name and prefix = :prefix'
                ),
                {'name': name, 'prefix': prefix, 'compressed': compressed}
            )
            conn.commit()

    @staticmethod
    def create(engine, name, rds_table_name, s3_prefix, schema):
        """
        Create a new record in the __Index table and return True if
        successful. Will overwrite any existing index with the same
        name.
        """
        assert s3_prefix.endswith('/'), "S3 prefix must be a common prefix ending with '/'"

        # add the new index to the table
        sql = (
            'INSERT INTO `__Indexes` (`name`, `table`, `prefix`, `schema`) '
            'VALUES (:name, :table, :prefix, :schema) '
            'ON DUPLICATE KEY UPDATE '
            '   `table` = VALUES(`table`), '
            '   `prefix` = VALUES(`prefix`), '
            '   `schema` = VALUES(`schema`), '
            # NULL, not 0: 0 stores the zero date, which reads back as a
            # non-null string and makes a re-created index look built
            '   `built` = NULL '
        )

        with engine.begin() as conn:
            row = conn.execute(text(sql), {'name': name, 'table': rds_table_name, 'prefix': s3_prefix, 'schema': schema})
            return row and row.lastrowid is not None

    @staticmethod
    def list_indexes(engine, filter_built=True):
        with engine.connect() as conn:
            """
            Return an iterator of all the indexes.
            """
            sql = 'SELECT `name`, `table`, `prefix`, `schema`, `built`, `compressed` FROM `__Indexes`'

            # convert all rows to an index definition
            indexes = map(lambda r: Index(*r), conn.execute(text(sql)).fetchall())

            # remove indexes not built?
            if filter_built:
                indexes = filter(lambda i: i.is_built, indexes)

            return indexes

    @staticmethod
    def lookup(engine, name, arity):
        """
        Lookup an index in the database, return its table name, s3 prefix,
        schema, etc.
        """
        sql = (
            f'SELECT `name`, `table`, `prefix`, `schema`, `built`, `compressed` '
            f'FROM `__Indexes` '
            f'WHERE `name` = :name AND {ARITY_SQL} = :arity'
        )

        with engine.connect() as conn:
            # lookup the index
            row = conn.execute(text(sql), {'name': name, 'arity': arity}).fetchone()

            if row is None:
                raise KeyError(f'No such index: {name}')

            return Index(*row)

    @staticmethod
    def lookup_all(engine, name):
        """
        Lookup an index in the database, return its table name, s3 prefix,
        schema, etc.
        """
        sql = (
            'SELECT `name`, `table`, `prefix`, `schema`, `built`, `compressed` '
            'FROM `__Indexes` '
            'WHERE `name` = :name'
        )

        with engine.connect() as conn:
            rows = conn.execute(text(sql), {'name': name}).fetchall()

            if len(rows) == 0:
                raise KeyError(f'No such index: {name}')

            return [Index(*row) for row in rows]

    @staticmethod
    def _swap_row(conn, name):
        """
        The one `__Indexes` row a swap should act on, or an error saying why
        there isn't one.

        A name may legitimately exist at several arities - `gene` is indexed
        both by name and by name and build. A swap names no arity, and the
        statements it runs match on name alone, so against such a name it
        would read one row arbitrarily and write all of them. Refuse instead.

        Runs on the caller's connection, and locks what it reads: the checks
        are only worth anything if the rows they passed are still the rows
        being written when the cutover runs.
        """
        sql = (
            f'SELECT `id`, `table`, `prefix`, `schema`, `built`, `compressed`, '
            f'{ARITY_SQL} AS `arity` FROM `__Indexes` WHERE `name` = :name FOR UPDATE'
        )

        rows = conn.execute(text(sql), {'name': name}).mappings().all()

        if not rows:
            raise KeyError(f'No such index: {name}')

        if len(rows) > 1:
            arities = ', '.join(str(r['arity']) for r in sorted(rows, key=lambda r: r['arity']))
            raise ValueError(
                f'{name} exists at more than one arity ({arities}); a swap cannot tell which '
                f'one it is meant to replace'
            )

        return rows[0]

    @staticmethod
    def swap_into(engine, temp_name, canonical_name):
        """
        Blue/green cutover: promote a built temp index into a name that is
        already being served.

        Repoints `__Keys` and `__Indexes` at the temp index's table in one
        transaction, so a reader sees either the whole old index or the whole
        new one, never a mixture of the two.

        Returns the table the canonical name used to point at. Dropping it is
        the caller's job, and has to happen after this returns: DDL commits
        implicitly in MySQL, so a DROP inside the cutover would end the
        transaction early and take the rollback with it.
        """
        # every statement below reads as a no-op against a single name, right
        # up to the last one, which deletes it - so the same name twice ends
        # with the index unpublished, its keys gone, and its live table handed
        # back to the caller to drop
        if temp_name == canonical_name:
            raise ValueError(f'{temp_name} cannot be swapped into itself')

        # the reads are inside the cutover, and locked, so that what the
        # checks below approved is what the writes below act on: read outside
        # it and a concurrent `create` could add a second arity between the
        # two, leaving the checks to pass on rows the writes never touch
        with engine.begin() as conn:
            temp = Index._swap_row(conn, temp_name)
            canonical = Index._swap_row(conn, canonical_name)

            if not _is_built(temp['built']):
                raise ValueError(f'{temp_name} has never been built; refusing to swap it in')

            if temp['arity'] != canonical['arity']:
                raise ValueError(
                    f'{temp_name} takes {temp["arity"]} query argument(s) but {canonical_name} '
                    f'takes {canonical["arity"]}; swapping would change what the name accepts'
                )

            # the canonical name's keys go first: `__Keys` is unique on
            # (index, key) and the temp index covers the same keys, so
            # renaming before deleting collides on every one of them
            conn.execute(text('DELETE FROM `__Keys` WHERE `index` = :name'),
                         {'name': canonical_name})
            conn.execute(text('UPDATE `__Keys` SET `index` = :canonical WHERE `index` = :temp'),
                         {'canonical': canonical_name, 'temp': temp_name})

            # by id, so a row added at another arity between the checks above
            # and here cannot widen these to rows the checks never saw
            conn.execute(text(
                'UPDATE `__Indexes` SET `table` = :table, `prefix` = :prefix, '
                '`schema` = :schema, `built` = :built, `compressed` = :compressed '
                'WHERE `id` = :id'),
                {'table': temp['table'], 'prefix': temp['prefix'], 'schema': temp['schema'],
                 'built': temp['built'], 'compressed': temp['compressed'],
                 'id': canonical['id']})
            conn.execute(text('DELETE FROM `__Indexes` WHERE `id` = :id'), {'id': temp['id']})

        return canonical['table']

    def prepare(self, engine, rebuild=False):
        """
        Ensure the records table exists for the index.
        """
        self.set_built_flag(engine, flag=False)

        if rebuild:
            self.delete_keys(engine)
            self.table.drop(engine, checkfirst=True)

        logging.info('Creating %s table...', self.table.name)
        self.table.create(engine, checkfirst=True)

    def build(self, config, engine, use_lambda=False, use_batch=False, use_grouped=False,
              group_size=50, group_max_bytes=2 * 1024 ** 3, workers=3,
              prefer_compressed=False, console=None):
        """
        Builds the index table for objects in S3.
        """
        logging.info('Finding keys in %s...', self.s3_prefix)
        s3_objects = list_index_objects(
            config.s3_bucket, config.s3_path(self.s3_prefix), prefer_compressed
        )

        # delete all stale keys; get the list of objects left to index
        objects = self.delete_stale_keys(config, engine, s3_objects, console=console)

        # calculate the total size of all the objects. grouped workers re-derive their chunk
        # from the full listing, so the bar tracks every object rather than just the stale ones.
        total_size = functools.reduce(lambda a, b: a + b['Size'],
                                      s3_objects if use_grouped else objects, 0)

        # progress format
        p_fmt = [
            "[progress.description]{task.description}",
            rich.progress.BarColumn(),
            rich.progress.FileSizeColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%"
        ]

        if objects:
            self.schema.drop_index(engine, self.table)

            # as each job finishes...
            with rich.progress.Progress(*p_fmt, console=console) as progress:
                overall = progress.add_task('[green]Indexing keys...[/]', total=total_size)

                # read several files in parallel
                pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers)

                # index the objects remotely using lambda or locally
                if use_lambda:
                    self.index_objects_remote(
                        config,
                        engine,
                        pool,
                        objects,
                        self.lambda_run_function,
                        progress,
                        overall,
                    )
                elif use_batch:
                    self.index_objects_remote(
                        config,
                        engine,
                        pool,
                        objects,
                        self.batch_run_function,
                        progress,
                        overall,
                    )
                elif use_grouped:
                    self.index_objects_grouped(
                        config, engine, s3_objects, prefer_compressed, group_size, group_max_bytes,
                        progress, overall,
                    )
                else:
                    self.index_objects_local(
                        config,
                        engine,
                        pool,
                        objects,
                        progress,
                        overall,
                    )

                # finally, build the index after all inserts are done
                logging.info('Building table index...')

            # each table knows how to build its own index
            self.schema.create_index(engine, self.table)

        # set the built flag for the index
        self.set_built_flag(engine, True)

        # done indexing
        logging.info('Index is up to date')

    def delete_stale_keys(self, config, engine, objects, console=None):
        """
        Deletes all records indexed where the key...

         - no longer exists
         - has the wrong version
         - hasn't been fully indexed
        """
        logging.info('Finding stale keys...')
        db_keys = self.lookup_keys(config, engine)
        # if a file in s3 is in the db but the version is different from what's in s3 we delete
        updated_files_for_db = [{'id': db_keys[o['Key']]['id'], 'key': o['Key']} for o in objects
                                if o['Key'] in db_keys and db_keys[o['Key']]['version'] != o['ETag'].strip('"')[:32]]
        updated_files_for_return = [o for o in objects if o['Key'] in set([f['key'] for f in updated_files_for_db])]
        s3_keys = set([o['Key'] for o in objects])
        deleted_files = [{'id': db_keys[k]['id'], 'key': k} for k in db_keys if k not in s3_keys]
        new_files = [o for o in objects if o['Key'] not in db_keys]
        # if a file is in the db but not in s3 we delete
        updated_or_deleted_files = updated_files_for_db + deleted_files

        if updated_or_deleted_files:
            with rich.progress.Progress(console=console) as progress:
                task = progress.add_task('[red]Deleting...[/]', total=len(updated_or_deleted_files))
                n = 0

                # delete stale or missing keys
                for kid in updated_or_deleted_files:
                    sql = f'DELETE FROM `{self.table.name}` WHERE `key` = :key'
                    with engine.begin() as conn:
                        n += conn.execute(text(sql), {'key': kid['id']}).rowcount

                    # remove the key from the __Keys table
                    self.delete_key(engine, kid['key'])
                    progress.advance(task)

                # show what was done
                logging.info(f'Deleted {n:,} records')
        else:
            logging.info('No stale keys; delete skipped')
        # return new and updated json files
        return new_files + updated_files_for_return

    def batch_run_function(self, config, obj):
        logging.info(f'Processing via batch {relative_key(obj["Key"], self.s3_prefix)}...')

        return start_and_wait_for_indexer_job(config.s3_path(obj['Key']), self.name, self.schema.arity,
                                              config.s3_bucket, config.rds_secret, config.bio_schema, obj['Size'])

    def lambda_run_function(self, config, obj):
        logging.info(f'Processing {relative_key(obj["Key"], self.s3_prefix)}...')

        # lambda function event data
        payload = {
            'index': self.name,
            'arity': self.schema.arity,
            'rds_secret': config.rds_secret,
            'rds_schema': config.bio_schema,
            's3_bucket': config.s3_bucket,
            's3_obj': obj,
        }

        # run the lambda asynchronously
        return invoke_lambda(config.lambda_function, payload)

    def index_objects_remote(self, config, engine, pool, objects, run_function, progress=None, overall=None):

        # create a job per object
        jobs = [pool.submit(run_function, config, obj) for obj in objects]

        # keys whose job came back without having indexed anything; collected
        # so one bad file doesn't hide the rest, and reported together at the end
        failures = []

        # as each job finishes, set the built flag for that key
        for job in concurrent.futures.as_completed(jobs):
            if job.exception() is not None:
                raise job.exception()

            result = job.result()
            # if result has 'key' then it's a lambda job
            if 'key' in result:
                key = result['key']
                record_count = result['records']
                size = result['size']
            else:
                key = result['parameters']['file']
                size = int(result['parameters']['file-size'])
                # not an easy way to get the number of records from batch and it's only used for logging
                record_count = 0

                # Batch returns a job description for FAILED just as it does for
                # SUCCEEDED - the waiter returns on either - so a failed child
                # arrives here looking like a finished one. Marking its key built
                # would record a file that was never indexed as done, and the
                # build would report success while missing data.
                if result.get('status') != 'SUCCEEDED':
                    logging.error('Indexer job for %s did not succeed (%s); not marking it built',
                                  key, result.get('statusReason'))
                    failures.append(key)

                    if progress:
                        progress.advance(overall, advance=size)

                    continue

            # the insert was done remotely, simply set the built flag now
            self.set_key_built_flag(engine, key)

            # output number of records
            logging.info(f'Wrote {record_count:,} records')

            # update the overall bar
            if progress:
                progress.advance(overall, advance=size)

        if failures:
            raise RuntimeError(
                f'{len(failures)} indexer job(s) failed and were not indexed: ' + ', '.join(failures)
            )

    def index_objects_grouped(self, config, engine, objects, prefer_compressed,
                              group_size, group_max_bytes, progress=None, overall=None):
        """Index objects via grouped Batch jobs, one per chunk.

        The parent chunks the full listing and passes each worker only coordinates
        (prefix, prefer_compressed, chunk_index, chunk_count, group bounds, expected_total);
        the worker re-lists S3 and re-runs _chunk_objects to recover the same chunk. No S3
        manifest is written, so the orchestrator needs no s3:PutObject. On SUCCESS the parent
        sets the per-key built flag for the chunk.
        """
        chunks = _chunk_objects(objects, group_size, group_max_bytes)
        n = len(chunks)
        expected_total = len(objects)

        # One failed chunk fails the build, so the moment we see one there is no reason to
        # keep waiting on the rest. Left to itself the executor would: sit in as_completed
        # until every sibling's Batch job reached a terminal state, and then, because a
        # raise inside `with` still runs shutdown(wait=True) on the way out, block again on
        # the same threads. On a 20-chunk index that hides the failure for as long as the
        # slowest sibling runs. So the failure is recorded rather than raised, the event
        # breaks the siblings out of their poll, and the raise happens after shutdown.
        abandon = threading.Event()
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(n, 16) or 1)
        failure = None
        try:
            future_to_chunk = {}
            for i, chunk in enumerate(chunks):
                fut = pool.submit(
                    start_and_wait_for_group_indexer_job, self.name, self.schema.arity,
                    config.s3_bucket, config.rds_secret, config.bio_schema,
                    config.s3_subdir, self.s3_prefix, prefer_compressed, i, n,
                    group_size, group_max_bytes, expected_total, abandon,
                )
                future_to_chunk[fut] = (i, chunk)
            for fut in concurrent.futures.as_completed(future_to_chunk):
                i, chunk = future_to_chunk[fut]
                job = fut.result()
                if job['status'] != 'SUCCEEDED':
                    reason = job.get('statusReason', 'grouped indexer job FAILED')
                    failure = f'chunk {i} of {n} ({len(chunk)} files): {reason}'
                    break
                for o in chunk:
                    self.set_key_built_flag(engine, o['Key'])
                    if progress:
                        progress.advance(overall, advance=o['Size'])
        finally:
            # wake the siblings before dropping the queued chunks; wait=False so an
            # in-flight poll cannot hold the raise back
            abandon.set()
            pool.shutdown(wait=False, cancel_futures=True)

        if failure is not None:
            raise RuntimeError(f'grouped indexer job failed for {self.name}: {failure}')

    def index_objects_local(self, config, engine, pool, objects, progress=None, overall=None):
        """
        Index S3 objects locally.
        """
        jobs = [pool.submit(self.index_object, engine, config.s3_bucket, obj, progress, overall) for obj in objects]

        # as each job finishes, insert the records into the table
        for job in concurrent.futures.as_completed(jobs):
            if job.exception() is not None:
                raise job.exception()

            # get the key and the record iterator returned
            key, records = job.result()

            # perform the insert serially, so jobs don't block each other
            self.insert_records_iter(engine, records)

            # after inserting, set the key as being built
            self.set_key_built_flag(engine, key)

    def index_object(self, engine, bucket, obj, progress=None, overall=None):
        """
        Read a file in S3, index it, and insert records into the table.
        """
        key, version, size = obj['Key'], obj['ETag'].strip('"')[:32], obj['Size']
        key_id = self.insert_key(engine, key, version)

        # read the file from s3
        content = read_lined_object(bucket, key)
        start_offset = 0
        records = {}

        # per-file progress bar
        rel_key = relative_key(key, self.s3_prefix)
        file_progress = progress and progress.add_task(f'[yellow]{rel_key}[/]', total=size)

        # process each line (record)
        for line_num, line in enumerate(content):
            row = orjson.loads(line)
            end_offset = start_offset + len(line.encode('utf-8')) + 1  # newline

            try:
                for key_tuple in self.schema.index_builder(row):
                    if key_tuple in records:
                        records[key_tuple]['end_offset'] = end_offset
                    else:
                        records[key_tuple] = {
                            'key': key_id,
                            'start_offset': start_offset,
                            'end_offset': end_offset,
                        }
            except (KeyError, ValueError) as e:
                logging.warning('%s; skipping...', e)

            # update progress
            if progress:
                progress.update(file_progress, completed=end_offset)

            # track current file offset
            start_offset = end_offset

        # done with this object; tick the overall progress
        if progress:
            progress.remove_task(file_progress)
            progress.advance(overall, advance=size)

        # NOTE: Because this is called as a job, be sure and return a iterator
        #       and not the records as this is memory that is kept around for
        #       the entire duration of indexing.
        return key, ({**self.schema.column_values(k), **r} for k, r in records.items())

    def _load_csv(self, engine, infile_name, quoted_fieldnames):
        """
        Bulk load a CSV into this index's table, retrying on deadlock -
        parallel builds lock each other out of the same table.
        """
        # the path goes into a SQL string literal; a quote in TMPDIR would
        # otherwise produce a statement that will not parse
        infile = infile_name.replace('\\', '/').replace("'", "''")

        sql = (
            f"LOAD DATA LOCAL INFILE '{infile}' "
            f"INTO TABLE `{self.table.name}` "
            f"FIELDS TERMINATED BY ',' "
            f"LINES TERMINATED BY '\\n' "
            f"IGNORE 1 ROWS "
            f"({','.join(quoted_fieldnames)}) "
        )

        # attempt to bulk load into the database
        for attempt in range(5):
            try:
                with engine.begin() as conn:
                    conn.execute(text(sql))
                return
            except sqlalchemy.exc.OperationalError as ex:
                # only lock contention is worth another go; anything else
                # would fail identically four more times
                if not _is_retryable(ex) or attempt == 4:
                    raise

                time.sleep(1)

    def insert_records(self, engine, records):
        """
        Insert all the records into the index table. It does this as fast as
        possible by writing the file to a CSV and then loading it directly
        into the table.
        """
        if len(records) == 0:
            return

        # get the field names from the first record
        fieldnames = list(records[0].keys())
        quoted_fieldnames = [f'`{field}`' for field in fieldnames]

        # create a temporary file to write the CSV to
        tmp = tempfile.NamedTemporaryFile(
            mode='w+t', delete=False, newline='', encoding='utf-8')

        try:
            try:
                # csv defaults to the excel dialect, whose terminator is '\r\n' -
                # but LOAD DATA is told LINES TERMINATED BY '\n', so the
                # stray '\r' lands in the last column of every row
                w = csv.DictWriter(tmp, fieldnames, lineterminator='\n')

                # write the header and the rows
                w.writeheader()
                w.writerows(records)
            finally:
                tmp.close()

            self._load_csv(engine, tmp.name, quoted_fieldnames)

            # output number of records
            logging.info(f'Wrote {len(records):,} records')
        finally:
            # the remove has to cover the write too, or a row that fails to
            # encode leaves the CSV behind for the life of the container
            os.remove(tmp.name)

    def insert_records_iter(self, engine, records):
        """
        Insert records from an iterator, streaming them to the CSV rather
        than materializing the whole sequence first.

        index_object yields a freshly merged dict per record, so listing them
        held every one of those at once: 54 MiB for a 200k-record file, where
        streaming costs 0.2 MiB. It does not bound total memory - the source
        dict index_object built stays alive until the iterator is drained -
        but the second copy is gone.
        """
        it = iter(records)

        # the field names come from the first record, so it has to be read
        # before the writer can be built - and no records means no load
        try:
            first = next(it)
        except StopIteration:
            return

        fieldnames = list(first.keys())
        quoted_fieldnames = [f'`{field}`' for field in fieldnames]

        tmp = tempfile.NamedTemporaryFile(
            mode='w+t', delete=False, newline='', encoding='utf-8')
        count = 0

        try:
            try:
                # csv defaults to the excel dialect, whose terminator is '\r\n' -
                # but LOAD DATA is told LINES TERMINATED BY '\n', so the
                # stray '\r' lands in the last column of every row
                w = csv.DictWriter(tmp, fieldnames, lineterminator='\n')

                w.writeheader()
                w.writerow(first)
                count = 1

                for record in it:
                    w.writerow(record)
                    count += 1
            finally:
                tmp.close()

            self._load_csv(engine, tmp.name, quoted_fieldnames)

            # output number of records
            logging.info(f'Wrote {count:,} records')
        finally:
            os.remove(tmp.name)

    def insert_records_batched(self, engine, records, batch_size=5000):
        """
        Insert all the records, but in batches. This way if multiple files are
        being indexed in parallel, they won't block each others' inserts by
        locking the table.
        """
        for i in range(0, len(records), batch_size):
            self.insert_records(engine, records[i:i + batch_size])

    def insert_key(self, engine, key, version):
        """
        Adds a key to the __Keys table for an index by name. If the key
        already exists and the versions match, just return the ID for it.
        If the versions don't match, delete the existing record and create
        a new one with a new ID.
        """
        sql = 'SELECT `id`, `version` FROM `__Keys` WHERE `index` = :index and `key` = :key'
        with engine.connect() as conn:
            row = conn.execute(text(sql), {'index': self.name, 'key': key}).fetchone()

            if row is not None:
                if row[1] == version:
                    return row[0]

                # delete the existing key entry
                conn.execute(text('DELETE FROM `__Keys` WHERE `id` = :id'), {'id': row[0]})

            # add a new entry
            sql = 'INSERT INTO `__Keys` (`index`, `key`, `version`) VALUES (:index, :key, :version)'
            row = conn.execute(text(sql), {'index': self.name, 'key': key, 'version': version})
            conn.commit()
            return row.lastrowid

    def delete_key(self, engine, key):
        """
        Removes all records from the index and the key from the __Keys
        table for a paritcular index/key pair.
        """
        sql = 'DELETE FROM `__Keys` WHERE `index` = :index and `key` = :key'
        with engine.begin() as conn:
            conn.execute(text(sql), {'index': self.name, 'key': key})

    def delete_keys(self, engine):
        """
        Removes all records from the __Keys table for a paritcular index
        by name.
        """
        with engine.begin() as conn:
            conn.execute(text('DELETE FROM `__Keys` WHERE `index` = :index'), {'index': self.name})

    def lookup_keys(self, config, engine):
        """
        Look up all the keys and versions for this index. Returns a dictionary
        key -> {id, version}. The version will be None if the key hasn't been
        completely indexed.
        """
        sql = 'SELECT `id`, `key`, `version`, `built` FROM `__Keys` WHERE `index` = :index AND `key` LIKE :prefix'
        with engine.begin() as conn:
            rows = conn.execute(text(sql), {'index': self.name, 'prefix': f"{config.s3_path(self.s3_prefix)}%"}).fetchall()

        return {key: {'id': id, 'version': built and ver} for id, key, ver, built in rows}

    def set_key_built_flag(self, engine, key):
        """
        Update the keys table to indicate the key has been built.
        """
        sql = 'UPDATE `__Keys` SET `built` = :built WHERE `index` = :index AND `key` = :key'
        with engine.begin() as conn:
            conn.execute(text(sql), {'index': self.name, 'key': key, 'built': datetime.datetime.utcnow()})

    def set_built_flag(self, engine, flag=True):
        """
        Update the __Index table to indicate this index has been built.
        """
        now = datetime.datetime.utcnow()
        with engine.begin() as conn:
            if flag:
                conn.execute(text('UPDATE `__Indexes` SET `built` = :built WHERE `name` = :name'),
                             {'name': self.name, 'built': now})
            else:
                conn.execute(text('UPDATE `__Indexes` SET `built` = NULL WHERE `name` = :name'), {'name': self.name})
