import concurrent.futures
import csv
import functools
import logging
import orjson
import os
import os.path
import rich.progress
import sqlalchemy
import tempfile

import lib.locus
import lib.s3
import lib.schema

from lib.utils import relative_key


def build(engine, index, bucket, s3_objects, console=None):
    """
    Builds the index table for objects in S3.
    """
    meta = sqlalchemy.MetaData()
    table = index.schema.build_table(index.table, meta)

    # clear the built flag for the index
    _set_built_flag(engine, index, False)

    # create the index table (drop any existing table already there)
    logging.info('Creating %s table...', table.name)
    table.drop(engine, checkfirst=True)
    table.create(engine)

    # TODO: Instead of nuking the table and starting over, delete records
    #       with keys that are deleted/out of date.

    # collect all the s3 objects into a list so the size is known
    objects = list(s3_objects)

    # make sure that there is data to index
    assert len(objects) > 0, 'No files found in S3 to index'

    # calculate the total size of all the objects
    total_size = functools.reduce(lambda a, b: a + b['Size'], objects, 0)

    # progress format
    p_fmt = [
        "[progress.description]{task.description}",
        rich.progress.BarColumn(),
        rich.progress.FileSizeColumn(),
        rich.progress.TransferSpeedColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%"
    ]

    # as each job finishes...
    with rich.progress.Progress(p_fmt, console=console) as progress:
        overall = progress.add_task('[green]Overall[/]', total=total_size)

        # read several files in parallel
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        jobs = [pool.submit(_index_object, bucket, obj, table, index, progress, overall) for obj in objects]

        # as each job finishes, insert the records into the table
        for job in concurrent.futures.as_completed(jobs):
            if job.exception() is not None:
                raise job.exception()

            # perform the insert serially, so jobs don't block each other
            _bulk_insert(engine, table, job.result())

        # finally, build the index after all inserts are done
        logging.info('Building table index...')

        # each table knows how to build its own index
        index.schema.build_index(engine, table)

        # set the built flag for the index
        _set_built_flag(engine, index, True)


def _index_object(bucket, obj, table, index, progress, overall):
    """
    Read a file in S3, index it, and insert records into the table.
    """
    key, size = obj['Key'], obj['Size']
    rel_pathname = relative_key(key, index.s3_prefix)

    # read the file from s3
    content = lib.s3.read_object(bucket, key)
    start_offset = 0
    records = {}

    # per-file progress bar
    file_progress = progress.add_task(f'[yellow]{rel_pathname}[/]', total=size)

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

    # transform all the records and collect them all into an insert batch
    return [{**index.schema.column_values(k), **r} for k, r in records.items()]


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
    engine.execute('UPDATE `__Indexes` SET `built` = %s WHERE `name` = %s', flag, index.name)
