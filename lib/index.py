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

import lib.create
import lib.locus
import lib.s3
import lib.schema


def build(engine, index, bucket, s3_objects, rebuild=False, cont=False, console=None):
    """
    Builds the index table for objects in S3.
    """
    meta = sqlalchemy.MetaData()
    table = index.schema.build_table(index.table, meta)

    # if continuing a build, use the current time
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    last_built = now if cont else _last_built(engine, index)

    # collect all the s3 objects into a list so the size is known
    objects = list(s3_objects)

    # force rebuild if never built or allow continue?
    if not rebuild:
        if not last_built:
            logging.error('Index not built; either rebuild or continue building')
            return

        # clean up stale keys and determine objects left to index
        objects = _delete_stale_keys(engine, table, objects, last_built)
    else:
        assert objects, 'No files found in S3 to index'

        # delete existing and create a new index
        logging.info('Creating %s table...', table.name)
        table.drop(engine, checkfirst=True)
        table.create(engine)

    # clear the built flag
    _set_built_flag(engine, index, False)

    # calculate the total size of all the objects
    total_size = functools.reduce(lambda a, b: a + b['Size'], objects, 0)

    # progress format
    p_fmt = [
        "[progress.description]{task.description}",
        rich.progress.BarColumn(),
        rich.progress.FileSizeColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%"
    ]

    # start indexing process
    logging.info('Indexing...' if objects else 'Index is up to date')

    # make sure the schema index doesn't exist while inserting
    if objects:
        index.schema.drop_index(engine, table)

        # as each job finishes...
        with rich.progress.Progress(*p_fmt, console=console) as progress:
            overall = progress.add_task('[green]Overall[/]', total=total_size)

            # read several files in parallel
            pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)
            jobs = [pool.submit(_index_object, bucket, obj, table, index, progress, overall) for obj in objects]

            # as each job finishes, insert the records into the table
            for job in concurrent.futures.as_completed(jobs):
                if job.exception() is not None:
                    raise job.exception()

                # perform the insert serially, so jobs don't block each other
                _bulk_insert(engine, table, list(job.result()))

            # finally, build the index after all inserts are done
            logging.info('Building table index...')

        # each table knows how to build its own index
        index.schema.create_index(engine, table)

    # set the built flag for the index
    _set_built_flag(engine, index, True)


def _last_built(engine, index):
    """
    Returns the last date/time the index was built.
    """
    sql = 'SELECT `built` FROM `__Indexes` WHERE `name` = %s LIMIT 1'
    row = engine.execute(sql, index.name).fetchone()

    if row is None:
        return None

    return row[0].replace(tzinfo=datetime.timezone.utc) if row[0] else None


def _delete_stale_keys(engine, table, objects, last_built):
    """
    Deletes all records indexed where the key...

     - no longer exists (not in the list of objects)
     - is newer than the last built timestamp of the index
     - hasn't been completely indexed
    """
    indexed_keys = set()

    # loop over the objects and discover the "stale" keys
    for obj in objects:
        key, size, date = obj['Key'], obj['Size'], obj['LastModified']
        logging.info('Checking %s...', key)

        # was this file previously indexed?
        if last_built > date:
            sql = f'SELECT MAX(end_offset) FROM `{table.name}` WHERE `key` = %s'
            row = engine.execute(sql, key).fetchone()

            # does the size match?
            if row and row[0] == size:
                indexed_keys.add(key)

    # if there are no valid indexed keys, just drop the table
    if not indexed_keys:
        logging.info('All keys are stale; rebuilding table')
        table.drop(engine, checkfirst=True)
        table.create(engine)
    else:
        logging.info('Deleting stale keys...')
        sql = f'DELETE FROM `{table.name}` WHERE `key` NOT IN (%s)'
        engine.execute(sql, indexed_keys)

    # return a list of objects that still need indexed
    return [o for o in objects if o['Key'] not in indexed_keys]


def _index_object(bucket, obj, table, index, progress, overall):
    """
    Read a file in S3, index it, and insert records into the table.
    """
    key, size = obj['Key'], obj['Size']

    # read the file from s3
    content = lib.s3.read_object(bucket, key)
    start_offset = 0
    records = {}

    # per-file progress bar
    rel_key = lib.s3.relative_key(key, index.s3_prefix)
    file_progress = progress.add_task(f'[yellow]{rel_key}[/]', total=size)

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
                        'key': key,
                        'start_offset': start_offset,
                        'end_offset': end_offset,
                    }
        except (KeyError, ValueError) as e:
            logging.warning('%s; skipping...', e)

        # update progress
        progress.update(file_progress, completed=end_offset)

        # track current file offset
        start_offset = end_offset

    # done with this object; tick the overall progress
    progress.remove_task(file_progress)
    progress.advance(overall, advance=size)

    # NOTE: Because this is called as a job, be sure and return a iterator
    #       and not the records as this is memory that is kept around for
    #       the entire duration of indexing.
    return ({**index.schema.column_values(k), **r} for k, r in records.items())


def _bulk_insert(engine, table, records):
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
            f"INTO TABLE `{table.name}` "
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


def _set_built_flag(engine, index, flag=True):
    """
    Update the index table to indicate the index has been built.
    """
    now = datetime.datetime.utcnow()

    if flag:
        engine.execute('UPDATE `__Indexes` SET `built` = %s WHERE `name` = %s', now, index.name)
    else:
        engine.execute('UPDATE `__Indexes` SET `built` = NULL WHERE `name` = %s', index.name)
