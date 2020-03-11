import concurrent.futures
import csv
import enlighten
import json
import logging
import os
import sqlalchemy
import tempfile

import lib.locus
import lib.s3
import lib.schema


def build(engine, table_name, schema, bucket, s3_objects):
    """
    Builds the index table for objects in S3.
    """
    meta = sqlalchemy.MetaData()
    table = schema.build_table(table_name, meta)

    # create the index table (drop any existing table already there)
    logging.info('Creating %s table...', table.name)
    table.drop(engine, checkfirst=True)
    table.create(engine)

    # collect all the s3 objects into a list so the size is known
    objects = list(s3_objects)

    # as each job finishes...
    with enlighten.get_manager() as progress_mgr:
        with progress_mgr.counter(total=len(objects), unit='files', series=' #') as overall_progress:
            for obj in objects:
                path, size = obj['Key'], obj['Size']
                logging.info('Processing %s...', path)

                # per-file progress of indexer
                with progress_mgr.counter(total=size // 1024, unit='KB', series=' #', leave=False) as file_progress:
                    _index_object(engine, bucket, path, table, schema, file_progress)

                # tick the overall progress
                overall_progress.update()

    # finally, build the index after all inserts are done
    logging.info('Building table index...')

    # each table knows how to build its own index
    schema.build_index(engine, table)


def _index_object(engine, bucket, path, table, schema, counter):
    """
    Read a file in S3, index it, and insert records into the table.
    """
    content = lib.s3.read_object(bucket, path)
    start_offset = 0
    records = {}

    # process each line (record)
    for line_num, line in enumerate(content.iter_lines()):
        row = json.loads(line)
        end_offset = start_offset + len(line) + 1  # newline

        try:
            for key_tuple in schema.index_keys(row):
                if key_tuple in records:
                    records[key_tuple]['end_offset'] = end_offset
                else:
                    records[key_tuple] = {
                        'path': path,
                        'start_offset': start_offset,
                        'end_offset': end_offset,
                    }

        except (KeyError, ValueError) as e:
            logging.warning('%s; skipping...', e)

        # update progress
        counter.update(incr=(end_offset // 1024) - counter.count)

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
            f"({','.join(fieldnames)}) "
        )

        # bulk load into the database
        engine.execute(sql)
    finally:
        os.remove(tmp.name)
