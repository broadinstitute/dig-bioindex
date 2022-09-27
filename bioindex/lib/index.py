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
import time

from .aws import invoke_lambda
from .s3 import list_objects, read_object, relative_key
from .schema import Schema
from .utils import cap_case_str


class Index:
    """
    An index definition that can be built or queried.
    """

    def __init__(self, name, table_name, s3_prefix, schema_string, built_date):
        """
        Initialize the index with everything needed to build keys and query.
        """
        self.schema = Schema(schema_string)
        self.table = self.schema.table_def(table_name, sqlalchemy.MetaData())
        self.name = name
        self.built = built_date
        self.s3_prefix = s3_prefix

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
            'VALUES (%s, %s, %s, %s) '
            'ON DUPLICATE KEY UPDATE '
            '   `table` = VALUES(`table`), '
            '   `prefix` = VALUES(`prefix`), '
            '   `schema` = VALUES(`schema`), '
            '   `built` = 0 '
        )

        # add to the database
        row = engine.execute(sql, name, rds_table_name, s3_prefix, schema)

        return row and row.lastrowid is not None

    @staticmethod
    def list_indexes(engine, filter_built=True):
        """
        Return an iterator of all the indexes.
        """
        sql = 'SELECT `name`, `table`, `prefix`, `schema`, `built` FROM `__Indexes`'

        # convert all rows to an index definition
        indexes = map(lambda r: Index(*r), engine.execute(sql))

        # remove indexes not built?
        if filter_built:
            indexes = filter(lambda i: i.built, indexes)

        return indexes

    @staticmethod
    def lookup(engine, name, arity):
        """
        Lookup an index in the database, return its table name, s3 prefix,
        schema, etc.
        """
        sql = (
            'SELECT `name`, `table`, `prefix`, `schema`, `built` '
            'FROM `__Indexes` '
            'WHERE `name` = %s AND LENGTH(`schema`) - LENGTH(REPLACE(`schema`, \',\', \'\')) + 1 = %s'
        )

        # lookup the index
        row = engine.execute(sql, name, arity).fetchone()

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
            'SELECT `name`, `table`, `prefix`, `schema`, `built` '
            'FROM `__Indexes` '
            'WHERE `name` = %s'
        )

        # lookup the index
        rows = engine.execute(sql, name).fetchall()

        if len(rows) == 0:
            raise KeyError(f'No such index: {name}')

        return [Index(*row) for row in rows]

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

    def build(self, config, engine, use_lambda=False, workers=3, console=None):
        """
        Builds the index table for objects in S3.
        """
        logging.info('Finding keys in %s...', self.s3_prefix)
        s3_objects = list(list_objects(config.s3_bucket, self.s3_prefix, exclude='_SUCCESS'))

        # delete all stale keys; get the list of objects left to index
        objects = self.delete_stale_keys(engine, s3_objects, console=console)

        # calculate the total size of all the objects
        total_size = functools.reduce(lambda a, b: a + b['Size'], objects, 0)

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
                        progress,
                        overall,
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

    def delete_stale_keys(self, engine, objects, console=None):
        """
        Deletes all records indexed where the key...

         - no longer exists
         - has the wrong version
         - hasn't been fully indexed
        """
        logging.info('Finding stale keys...')
        keys = self.lookup_keys(engine)

        # all keys are considered stale initially
        stale_ids = set(map(lambda k: k['id'], keys.values()))
        indexed_keys = set()

        # loop over all the valid objects to be indexed
        for obj in objects:
            key, version = obj['Key'], obj['ETag'].strip('"')
            k = keys.get(key)

            # is this key already built and match versions?
            if k and k['version'] == version:
                stale_ids.remove(k['id'])
                indexed_keys.add(key)

        # delete stale keys
        if stale_ids:
            # if all the keys are stale, just drop the table
            if not indexed_keys:
                logging.info(f'Deleting table...')
                self.prepare(engine, rebuild=True)
            else:
                with rich.progress.Progress(console=console) as progress:
                    task = progress.add_task('[red]Deleting...[/]', total=len(stale_ids))
                    n = 0

                    # delete all the keys from the table
                    for kid in stale_ids:
                        sql = f'DELETE FROM {self.table.name} WHERE `key` = %s'
                        n += engine.execute(sql, kid).rowcount

                        # remove the key from the __Keys table
                        self.delete_key(engine, key)
                        progress.advance(task)

                    # show what was done
                    logging.info(f'Deleted {n:,} records')
        else:
            logging.info('No stale keys; delete skipped')

        # filter the objects that still need to be indexed
        return [o for o in objects if o['Key'] not in indexed_keys]

    def index_objects_remote(self, config, engine, pool, objects, progress=None, overall=None):
        """
        Index the objects using a lambda function.
        """
        def run_function(obj):
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

        # create a job per object
        jobs = [pool.submit(run_function, obj) for obj in objects]

        # as each job finishes, set the built flag for that key
        for job in concurrent.futures.as_completed(jobs):
            if job.exception() is not None:
                raise job.exception()

            result = job.result()
            key = result['key']
            record_count = result['records']
            size = result['size']

            # the insert was done remotely, simply set the built flag now
            self.set_key_built_flag(engine, key)

            # output number of records
            logging.info(f'Wrote {record_count:,} records')

            # update the overall bar
            if progress:
                progress.advance(overall, advance=size)

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
            self.insert_records(engine, list(records))

            # after inserting, set the key as being built
            self.set_key_built_flag(engine, key)

    def index_object(self, engine, bucket, obj, progress=None, overall=None):
        """
        Read a file in S3, index it, and insert records into the table.
        """
        key, version, size = obj['Key'], obj['ETag'].strip('"'), obj['Size']
        key_id = self.insert_key(engine, key, version)

        # read the file from s3
        content = read_object(bucket, key)
        start_offset = 0
        records = {}

        # per-file progress bar
        rel_key = relative_key(key, self.s3_prefix)
        file_progress = progress and progress.add_task(f'[yellow]{rel_key}[/]', total=size)

        # process each line (record)
        for line_num, line in enumerate(content.iter_lines()):
            row = orjson.loads(line)
            end_offset = start_offset + len(line) + 1  # newline

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
        tmp = tempfile.NamedTemporaryFile(mode='w+t', delete=False)

        try:
            w = csv.DictWriter(tmp, fieldnames)

            # write the header and the rows
            w.writeheader()
            w.writerows(records)
        finally:
            tmp.close()

        try:
            infile = tmp.name.replace('\\', '/')
            fail_ex = None

            sql = (
                f"LOAD DATA LOCAL INFILE '{infile}' "
                f"INTO TABLE `{self.table.name}` "
                f"FIELDS TERMINATED BY ',' "
                f"LINES TERMINATED BY '\\n' "
                f"IGNORE 1 ROWS "
                f"({','.join(quoted_fieldnames)}) "
            )

            # attempt to bulk load into the database
            for _ in range(5):
                try:
                    engine.execute(sql)
                    break
                except sqlalchemy.exc.OperationalError as ex:
                    fail_ex = ex
                    if ex.code == 1213:  # deadlock; wait and try again
                        time.sleep(1)
            else:
                # failed to insert the rows, die
                raise fail_ex

            # output number of records
            logging.info(f'Wrote {len(records):,} records')
        finally:
            os.remove(tmp.name)

    def insert_records_batched(self, engine, records, batch_size=5000):
        """
        Insert all the records, but in batches. This way if multiple files are
        being indexed in parallel, they won't block each others' inserts by
        locking the table.
        """
        for i in range(0, len(records), batch_size):
            self.insert_records(engine, records[i:i+batch_size])

    def insert_key(self, engine, key, version):
        """
        Adds a key to the __Keys table for an index by name. If the key
        already exists and the versions match, just return the ID for it.
        If the versions don't match, delete the existing record and create
        a new one with a new ID.
        """
        sql = 'SELECT `id`, `version` FROM `__Keys` WHERE `index` = %s and `key` = %s'
        row = engine.execute(sql, self.name, key).fetchone()

        if row is not None:
            if row[1] == version:
                return row[0]

            # delete the existing key entry
            engine.execute('DELETE FROM `__Keys` WHERE `id` = %s', row[0])

        # add a new entry
        sql = 'INSERT INTO `__Keys` (`index`, `key`, `version`) VALUES (%s, %s, %s)'
        row = engine.execute(sql, self.name, key, version)

        return row.lastrowid

    def delete_key(self, engine, key):
        """
        Removes all records from the index and the key from the __Keys
        table for a paritcular index/key pair.
        """
        sql = 'DELETE FROM `__Keys` WHERE `index` = %s and `key` = %s'
        engine.execute(sql, self.name, key)

    def delete_keys(self, engine):
        """
        Removes all records from the __Keys table for a paritcular index
        by name.
        """
        engine.execute('DELETE FROM `__Keys` WHERE `index` = %s', self.name)

    def lookup_keys(self, engine):
        """
        Look up all the keys and versions for this index. Returns a dictionary
        key -> {id, version}. The version will be None if the key hasn't been
        completely indexed.
        """
        sql = 'SELECT `id`, `key`, `version`, `built` FROM `__Keys` WHERE `index` = %s AND `key` LIKE %s'
        rows = engine.execute(sql, self.name, f'{self.s3_prefix}%').fetchall()

        return {key: {'id': id, 'version': built and ver} for id, key, ver, built in rows}

    def set_key_built_flag(self, engine, key):
        """
        Update the keys table to indicate the key has been built.
        """
        sql = 'UPDATE `__Keys` SET `built` = %s WHERE `index` = %s AND `key` = %s'
        engine.execute(sql, datetime.datetime.utcnow(), self.name, key)

    def set_built_flag(self, engine, flag=True):
        """
        Update the __Index table to indicate this index has been built.
        """
        now = datetime.datetime.utcnow()

        if flag:
            engine.execute('UPDATE `__Indexes` SET `built` = %s WHERE `name` = %s', now, self.name)
        else:
            engine.execute('UPDATE `__Indexes` SET `built` = NULL WHERE `name` = %s', self.name)
