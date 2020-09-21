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

from .aws import invoke_lambda
from .s3 import read_object, relative_key
from .tables import create_keys_table, delete_key, delete_keys, insert_key, lookup_keys


def build(engine, cfg, index, s3_objects, use_lambda=False, rebuild=False, cont=False, workers=3, console=None):
    """
    Builds the index table for objects in S3.
    """
    create_keys_table(engine)

    # build the index table definition
    meta = sqlalchemy.MetaData()
    table = index.schema.table_def(index.table, meta)

    # if continuing a build, use the current time
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    last_built = now if cont else _last_built(engine, index)

    # collect all the s3 objects into a list so the size is known
    logging.info('Finding keys in %s...', index.s3_prefix)
    objects = list(s3_objects)

    # force rebuild if never built or allow continue?
    if not rebuild:
        if not last_built:
            logging.error('Index not built; either rebuild or continue building')
            return

        # clean up stale keys and determine objects left to index
        objects = _delete_stale_keys(engine, index, table, objects, last_built, console)
    else:
        assert objects, 'No files found in S3 to index'

        # remove all __Keys indexed already since starting clean
        delete_keys(engine, index.name)

        # delete existing and create a new index
        logging.info('Creating %s table...', table.name)
        table.drop(engine, checkfirst=True)
        table.create(engine)

    # clear the built flag
    _set_index_built_flag(engine, index, False)

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
        index.schema.drop_index(engine, table)

        # as each job finishes...
        with rich.progress.Progress(*p_fmt, console=console) as progress:
            overall = progress.add_task('[green]Indexing...[/]', total=total_size)

            # read several files in parallel
            pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers)

            # index the objects remotely using lambda or locally
            if use_lambda:
                _index_objects_remote(engine, cfg, pool, objects, index, cfg.lambda_function)
            else:
                _index_objects_local(engine, cfg, pool, objects, index, progress, overall)

            # finally, build the index after all inserts are done
            logging.info('Building table index...')

        # each table knows how to build its own index
        index.schema.create_index(engine, table)

    # set the built flag for the index
    _set_index_built_flag(engine, index, True)

    # done indexing
    logging.info('Index is up to date')


def _last_built(engine, index):
    """
    Returns the last date/time the index was built.
    """
    sql = 'SELECT `built` FROM `__Indexes` WHERE `name` = %s LIMIT 1'
    row = engine.execute(sql, index.name).fetchone()

    if row is None:
        return None

    return row[0].replace(tzinfo=datetime.timezone.utc) if row[0] else None


def _delete_stale_keys(engine, index, table, objects, last_built, console):
    """
    Deletes all records indexed where the key...

     - no longer exists
     - has the wrong version
     - hasn't been fully indexed
    """
    logging.info('Finding stale keys...')
    keys = lookup_keys(engine, index.name)

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
        # TODO: if all the keys are stale, just drop the table

        with rich.progress.Progress(console=console) as progress:
            task = progress.add_task('[red]Deleting...[/]', total=len(stale_ids))
            n = 0

            # delete all the keys from the table
            for kid in stale_ids:
                sql = f'DELETE FROM {table.name} WHERE `key` = %s'
                n += engine.execute(sql, kid).rowcount

                # remove the key from the __Keys table
                delete_key(engine, index.name, key)
                progress.advance(task)

        # show what was done
        logging.info(f'Deleted {n:,} records')
    else:
        logging.info('No stale keys; delete skipped')

    # filter the objects that still need to be indexed
    return [o for o in objects if o['Key'] not in indexed_keys]


def _index_objects_remote(engine, cfg, pool, objects, index, function_name):
    """
    Index the objects using a lambda.
    """
    def make_payload(obj):
        return {
            'index': index.name,
            'rds_instance': cfg.rds_instance,
            'rds_schema': cfg.bio_schema,
            's3_bucket': cfg.s3_bucket,
            's3_obj': obj,
        }

    # create a job per object
    jobs = [pool.submit(invoke_lambda, function_name, make_payload(obj)) for obj in objects]

    # as each job finishes, set the built flag for that key
    for job in concurrent.futures.as_completed(jobs):
        if job.exception() is not None:
            raise job.exception()

        result = job.result()
        key = result['key']
        record_count = result['records']

        # the insert was done remotely, simply set the built flag now
        _set_key_built_flag(engine, index, key)

        # output number of records
        logging.info(f'Wrote {record_count:,} records')


def _index_objects_local(engine, cfg, pool, objects, index, progress, overall):
    """
    Index the objects locally.
    """
    jobs = [pool.submit(_index_object, engine, cfg.s3_bucket, obj, index, progress, overall) for obj in objects]

    # as each job finishes, insert the records into the table
    for job in concurrent.futures.as_completed(jobs):
        if job.exception() is not None:
            raise job.exception()

        # get the key and the record iterator returned
        key, records = job.result()

        # perform the insert serially, so jobs don't block each other
        _bulk_insert(engine, index.table, list(records))

        # after inserting, set the key as being built
        _set_key_built_flag(engine, index, key)


def _index_object(engine, bucket, obj, index, progress=None, overall=None):
    """
    Read a file in S3, index it, and insert records into the table.
    """
    key, version, size = obj['Key'], obj['ETag'].strip('"'), obj['Size']
    key_id = insert_key(engine, index.name, key, version)

    # read the file from s3
    content = read_object(bucket, key)
    start_offset = 0
    records = {}

    # per-file progress bar
    rel_key = relative_key(key, index.s3_prefix)
    file_progress = progress and progress.add_task(f'[yellow]{rel_key}[/]', total=size)

    # process each line (record)
    for line_num, line in enumerate(content.iter_lines()):
        row = orjson.loads(line)
        end_offset = start_offset + len(line) + 1  # newline

        try:
            for key_tuple in index.schema.index_builder(row):
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
    return key, ({**index.schema.column_values(k), **r} for k, r in records.items())


def _bulk_insert(engine, table_name, records):
    """
    Insert all the records in batches.
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

        sql = (
            f"LOAD DATA LOCAL INFILE '{infile}' "
            f"INTO TABLE `{table_name}` "
            f"FIELDS TERMINATED BY ',' "
            f"LINES TERMINATED BY '\\n' "
            f"IGNORE 1 ROWS "
            f"({','.join(quoted_fieldnames)}) "
        )

        # bulk load into the database
        engine.execute(sql)

        # output number of records
        logging.info(f'Wrote {len(records):,} records')
    finally:
        os.remove(tmp.name)


def _set_key_built_flag(engine, index, key):
    """
    Update the keys table to indicate the key has been built.
    """
    sql = 'UPDATE `__Keys` SET `built` = %s WHERE `index` = %s AND `key` = %s'

    now = datetime.datetime.utcnow()
    engine.execute(sql, now, index.name, key)


def _set_index_built_flag(engine, index, flag=True):
    """
    Update the index table to indicate the index has been built.
    """
    now = datetime.datetime.utcnow()

    if flag:
        engine.execute('UPDATE `__Indexes` SET `built` = %s WHERE `name` = %s', now, index.name)
    else:
        engine.execute('UPDATE `__Indexes` SET `built` = NULL WHERE `name` = %s', index.name)