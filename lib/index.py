import csv
import enlighten
import json
import logging
import os
import sqlalchemy
import tempfile

import lib.locus
import lib.metadata
import lib.s3
import lib.schema


def build(engine, table, schema, bucket, s3_objects):
    """
    Build an index table for a set of objects in an S3 bucket using a schema.
    The schema is a string that is either a locus of column names or a single
    field. Example schemas:

      * chromosome:position
      * chromosome:start-end
      * phenotype
      * varId
    """
    locus_class, locus_cols = lib.locus.parse_columns(schema)

    if locus_class:
        return _index_objects(
            engine,
            table,
            schema,
            bucket,
            s3_objects,
            lambda row: locus_class(*(row.get(col) for col in locus_cols if col)).loci(),
            lambda k: {'chromosome': k[0], 'position': k[1]},
        )
    else:
        return _index_objects(
            engine,
            table,
            schema,
            bucket,
            s3_objects,
            lambda row: [row[schema]],
            lambda k: {'value': k},
        )


def _index_objects(engine, table, schema, bucket, s3_objects, keys, index_keys):
    """
    Builds the index table for objects in S3.
    """
    lib.metadata.update(engine, table.name, schema)

    # create the index table (drop any existing table already there)
    logging.info('Creating %s table...', table.name)
    table.drop(engine, checkfirst=True)
    table.create(engine)

    # collect all the s3 objects into a list so the size is known
    objects = list(s3_objects)

    # progress bar management
    with enlighten.get_manager() as progress_mgr:
        overall_progress = progress_mgr.counter(total=len(objects), unit='files', series=' #')

        # process each s3 object
        for obj in objects:
            path, size = obj['Key'], obj['Size']
            logging.info('Processing %s...', path)

            # create progress bar for each file
            file_progress = progress_mgr.counter(total=size // 1024, unit='KB', series=' #', leave=False)

            # stream the file from s3
            content = lib.s3.read_object(bucket, path)
            start_offset = 0
            records = {}

            # process each line (record)
            for line_num, line in enumerate(content.iter_lines()):
                row = json.loads(line)
                end_offset = start_offset + len(line) + 1  # newline

                try:
                    for k in keys(row):
                        if k in records:
                            records[k]['end_offset'] = end_offset
                        else:
                            records[k] = {
                                'path': path,
                                'start_offset': start_offset,
                                'end_offset': end_offset,
                            }

                except (KeyError, ValueError) as e:
                    logging.warning('%s; skipping...', e)

                # update the progress bar
                file_progress.update(incr=(end_offset // 1024) - file_progress.count)

                # track current file offset
                start_offset = end_offset

            # transform all the records and collect them all into an insert batch
            batch = [{**index_keys(k), **r} for k, r in records.items()]

            # perform the insert in batches
            _batch_insert(engine, table, batch)

            # update progress
            file_progress.close()
            overall_progress.update()

        # show the number of records attempting to be inserted
        logging.info('Building table index...')

        # finally, build the index after all inserts are done (if locus)
        sqlalchemy.Index('locus_idx', table.c.chromosome, table.c.position).create(engine)

        # done
        overall_progress.close()


def _batch_insert(engine, table, records):
    """
    Insert all the records in batches.
    """
    logging.info(f'Writing {len(records):,} records...')

    if len(records) == 0:
        return

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
