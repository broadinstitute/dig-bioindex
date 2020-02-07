import json
import logging
import sqlalchemy
import sys

import lib.locus
import lib.s3


def by_locus(engine, table, locus, bucket, s3_objects):
    """
    Index table records in s3 to redis.
    """
    locus_cols = lib.locus.parse_columns(locus)
    locus_class = lib.locus.SNPLocus if locus_cols[2] is None else lib.locus.RegionLocus

    # drop and create the table
    table.drop(engine, checkfirst=True)
    table.create(engine)

    # tally record count
    n = 0

    # list all the input tables
    for obj in s3_objects:
        path, tag = obj['Key'], obj['ETag']

        # stream the file from s3
        # reader = smart_open.open(lib.s3.uri(bucket, path))
        content = lib.s3.read_object(bucket, path)
        offset = 0
        records = {}

        # accumulate the records
        for line_num, line in enumerate(content.iter_lines()):
            sys.stderr.write(f'Processing {path} line {(line_num+1):,}...\r')
            row = json.loads(line)

            try:
                locus_obj = locus_class(*(row.get(col) for col in locus_cols if col))

                # add new loci and expand existing
                for locus in locus_obj.loci():
                    if locus in records:
                        records[locus]['length'] = offset + len(line) - records[locus]['offset']
                    else:
                        records[locus] = {
                            'path': path,
                            'offset': offset,
                            'length': len(line) + 1,
                        }

            except (KeyError, ValueError) as e:
                sys.stderr.write('\n')
                logging.warning('%s; skipping...', e)

            # track current file offset
            offset += len(line) + 1  # newline character

        # transform all the records
        batch = [{'chromosome': locus[0], 'position': locus[1], **r} for locus, r in records.items()]

        # show the number of records attempting to be inserted
        sys.stderr.write('\n')
        logging.info(f'Inserting {len(records):,} records...')

        # perform insert
        resp = engine.execute(table.insert(values=batch))
        n += resp.rowcount

    # show the number of records attempting to be inserted
    logging.info('Building table index...')

    # build the index after all inserts
    sqlalchemy.Index('locus_idx', table.c.chromosome, table.c.position).create(engine)

    return n
