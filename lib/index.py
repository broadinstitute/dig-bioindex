import csv
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

    # as each job finishes...
    with rich.progress.Progress(console=console) as progress_mgr:
        overall_progress = progress_mgr.add_task('[green]Overall[/]', total=len(objects))

        for obj in objects:
            key, size = obj['Key'], obj['Size']
            logging.info('Processing %s...', os.path.basename(key))

            # TODO: check if this key is already indexed, if so, skip it

            # per-file progress bar
            file_progress = progress_mgr.add_task('[cyan]File[/]', total=size // 1024)

            # index the entire file
            _index_object(engine, bucket, key, table, index.schema, progress_mgr, file_progress)

            # done with this file; tick the overall progress
            progress_mgr.remove_task(file_progress)
            progress_mgr.advance(overall_progress)

        # finally, build the index after all inserts are done
        logging.info('Building table index...')

        # each table knows how to build its own index
        index.schema.build_index(engine, table)

        # set the built flag for the index
        _set_built_flag(engine, index, True)


def _index_object(engine, bucket, key, table, schema, progress_mgr, task):
    """
    Read a file in S3, index it, and insert records into the table.
    """
    content = lib.s3.read_object(bucket, key)
    start_offset = 0
    records = {}

    # process each line (record)
    for line_num, line in enumerate(content.iter_lines()):
        row = orjson.loads(line)
        end_offset = start_offset + len(line) + 1  # newline

        try:
            for key_tuple in schema.index_builder(row):
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
        progress_mgr.update(task, completed=end_offset // 1024)

        # track current file offset
        start_offset = end_offset

    # transform all the records and collect them all into an insert batch
    batch = [{**schema.column_values(k), **r} for k, r in records.items()]

    # perform the insert in batches
    _bulk_insert(engine, table, batch)


def _bulk_insert(engine, table, records):
    """
    Insert all the records in batches.
    """
    if len(records) == 0:
        return

    # output number of records
    logging.info(f'Writing {len(records):,} records...')

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
    finally:
        os.remove(tmp.name)


def _set_built_flag(engine, index, flag=True):
    """

    """
    engine.execute('UPDATE `__Indexes` SET `built` = %s WHERE `name` = %s', flag, index.name)
