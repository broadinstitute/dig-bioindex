import enlighten
import json
import logging
import sqlalchemy

import lib.locus
import lib.metadata
import lib.s3
import lib.schema


def build(engine, table, schema, bucket, s3_objects):
    """
    Build an index table for a set of objects in an S3 bucket using a schema.
    """
    locus_class, locus_cols = lib.locus.parse_columns(schema)

    if locus_class:
        return _by_locus(engine, table, schema, bucket, s3_objects)
    else:
        return _by_value(engine, table, schema, bucket, s3_objects)


def _by_locus(engine, table, locus, bucket, s3_objects):
    """
    Index table records in s3 to redis.
    """
    locus_class, locus_cols = lib.locus.parse_columns(locus)

    # create the metadata table if it doesn't exist yet
    lib.metadata.update(engine, table.name, locus)

    # drop and create the table
    logging.info('Creating %s table...', table.name)
    table.drop(engine, checkfirst=True)
    table.create(engine)

    # collect all the s3 objects
    objects = list(s3_objects)

    # progress bar management
    with enlighten.get_manager() as progress_mgr:
        overall_progress = progress_mgr.counter(total=len(objects), unit='files')

        # list all the input tables
        for obj in objects:
            path, size, tag = obj['Key'], obj['Size'], obj['ETag']
            logging.info('Processing %s...', path)

            # create line progress bar
            file_progress = progress_mgr.counter(total=size // 1024, unit='KB', leave=False)

            # stream the file from s3
            content = lib.s3.read_object(bucket, path)
            start_offset = 0
            records = {}

            # process the records
            for line_num, line in enumerate(content.iter_lines()):
                row = json.loads(line)
                end_offset = start_offset + len(line) + 1  # newline

                try:
                    locus_obj = locus_class(*(row.get(col) for col in locus_cols if col))

                    # add new loci or expand existing
                    for locus in locus_obj.loci():
                        if locus in records:
                            records[locus]['end_offset'] = end_offset
                        else:
                            records[locus] = {
                                'path': path,
                                'start_offset': start_offset,
                                'end_offset': end_offset,
                            }

                except (KeyError, ValueError) as e:
                    logging.warning('%s; skipping...', e)

                # update the progress bar
                file_progress.update(incr=(end_offset-start_offset) // 1024)

                # track current file offset
                start_offset = end_offset

            # transform all the records
            batch = [{'chromosome': locus[0], 'position': locus[1], **r} for locus, r in records.items()]

            # perform the insert in batches
            _batch_insert(engine, table, batch, progress_mgr=progress_mgr)

            # done processing the file
            file_progress.close()
            overall_progress.update()

        # show the number of records attempting to be inserted
        logging.info('Building table index...')

        # finally, build the index after all inserts are done
        sqlalchemy.Index('locus_idx', table.c.chromosome, table.c.position).create(engine)

        # done
        overall_progress.close()


def _by_value(engine, table, column, bucket, s3_objects):
    """
    Index table records in s3 to redis.
    """
    lib.metadata.update(engine, table.name, column)

    # drop and create the table
    logging.info('Creating %s table...', table.name)
    table.drop(engine, checkfirst=True)
    table.create(engine)

    # collect all the s3 objects
    objects = list(s3_objects)

    # progress bar management
    with enlighten.get_manager() as progress_mgr:
        overall_progress = progress_mgr.counter(total=len(objects), unit='files')

        # list all the input tables
        for obj in objects:
            path, size, tag = obj['Key'], obj['Size'], obj['ETag']
            logging.info('Processing %s...', path)

            # create line progress bar
            file_progress = progress_mgr.counter(total=size // 1024, unit='KB', leave=False)

            # stream the file from s3
            content = lib.s3.read_object(bucket, path)
            start_offset = 0
            records = {}

            # process the records
            for line_num, line in enumerate(content.iter_lines()):
                row = json.loads(line)
                end_offset = start_offset + len(line) + 1  # newline

                try:
                    value = row[column]

                    # add new value or expand
                    if value in records:
                        records[value]['end_offset'] = end_offset
                    else:
                        records[value] = {
                            'path': path,
                            'start_offset': start_offset,
                            'end_offset': end_offset,
                            'value': value,
                        }

                except KeyError as e:
                    logging.warning('%s; skipping...', e)

                # update the progress bar
                file_progress.update(incr=(end_offset-start_offset) // 1024)

                # track current file offset
                start_offset = end_offset

            # perform the insert in batches
            _batch_insert(engine, table, list(records.values()), progress_mgr=progress_mgr)

            # done processing the file
            file_progress.close()
            overall_progress.update()

        # show the number of records attempting to be inserted
        logging.info('Building table index...')

        # finally, build the index after all inserts are done
        sqlalchemy.Index('locus_idx', table.c.value).create(engine)

        # done
        overall_progress.close()


def _batch_insert(engine, table, records, batch_size=10000, progress_mgr=None):
    """
    Insert all the records in batches.
    """
    counter = progress_mgr and progress_mgr.counter(total=len(records), unit='records', leave=False)

    for i in range(0, len(records), batch_size):
        rows = engine.execute(table.insert(values=records[i:i+batch_size])).rowcount

        # keep a running log of inserts to show progress
        if counter:
            counter.update(incr=rows)

    if counter:
        counter.close()
