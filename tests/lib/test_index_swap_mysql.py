"""
The cutover itself, against a real MySQL.

swap_into is almost entirely a claim about a transaction and about the
(name, arity) unique index on `__Indexes` - neither of which a fake engine
can be wrong about in the same way MySQL is. The tables here are built by
migrate.py rather than by hand so the constraints under test are the ones
production actually runs.

Point BIOINDEX_TEST_MYSQL_URL at a throwaway server to run these:

    docker run -d --name bioindex-swap-test -e MYSQL_ROOT_PASSWORD=test \\
        -e MYSQL_DATABASE=bio -p 13306:3306 mysql:8.0 \\
        --sql-mode=NO_ENGINE_SUBSTITUTION
    BIOINDEX_TEST_MYSQL_URL=mysql+pymysql://root:test@127.0.0.1:13306/bio \\
        python -m pytest tests/lib/test_index_swap_mysql.py
"""
import datetime
import os

import pytest
import sqlalchemy
from sqlalchemy import text

from bioindex.lib import migrate
from bioindex.lib.index import Index

URL = os.environ.get('BIOINDEX_TEST_MYSQL_URL')

pytestmark = pytest.mark.skipif(
    not URL, reason='BIOINDEX_TEST_MYSQL_URL is not set; see this module docstring')

BUILT = datetime.datetime(2026, 8, 3, 12, 0, 0)


@pytest.fixture
def engine():
    """A database holding nothing but the tables migrate.py creates."""
    engine = sqlalchemy.create_engine(URL)

    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS `__Keys`'))
        conn.execute(text('DROP TABLE IF EXISTS `__Indexes`'))

    migrate.create_indexes_table(engine)
    migrate.index_migration_1(engine)
    migrate.create_keys_table(engine)

    return engine


def add_index(engine, name, table, schema, built=BUILT):
    with engine.begin() as conn:
        conn.execute(text(
            'INSERT INTO `__Indexes` (`name`, `table`, `prefix`, `schema`, `built`) '
            'VALUES (:name, :table, :prefix, :schema, :built)'),
            {'name': name, 'table': table, 'prefix': f'{name}/',
             'schema': schema, 'built': built})


def add_key(engine, index_name, key, version='v1'):
    with engine.begin() as conn:
        conn.execute(text(
            'INSERT INTO `__Keys` (`index`, `key`, `version`, `built`) '
            'VALUES (:index, :key, :version, :built)'),
            {'index': index_name, 'key': key, 'version': version, 'built': BUILT})


def indexes(engine):
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(text(
            'SELECT `name`, `table`, `schema` FROM `__Indexes` ORDER BY `id`'))]


def keys(engine):
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(text(
            'SELECT `index`, `key`, `version` FROM `__Keys` ORDER BY `index`, `key`'))]


def test_the_canonical_name_serves_the_temp_table_afterwards(engine):
    add_index(engine, 'assoc', 'assoc_old', 'phenotype,chromosome:position')
    add_index(engine, 'assoc-tmp', 'assoc_new',
              'phenotype,chromosome:position;locus_step=250')

    old_table = Index.swap_into(engine, 'assoc-tmp', 'assoc')

    assert old_table == 'assoc_old'
    assert indexes(engine) == [
        ('assoc', 'assoc_new', 'phenotype,chromosome:position;locus_step=250')]


def test_the_keys_move_with_it(engine):
    add_index(engine, 'assoc', 'assoc_old', 'phenotype')
    add_index(engine, 'assoc-tmp', 'assoc_new', 'phenotype')
    add_key(engine, 'assoc', 'a/1.json', version='old')
    add_key(engine, 'assoc-tmp', 'a/1.json', version='new')
    add_key(engine, 'assoc-tmp', 'a/2.json', version='new')

    Index.swap_into(engine, 'assoc-tmp', 'assoc')

    # the canonical name now claims exactly the keys the temp index built,
    # at the versions it built them from
    assert keys(engine) == [
        ('assoc', 'a/1.json', 'new'),
        ('assoc', 'a/2.json', 'new'),
    ]


def test_the_swapped_in_index_is_the_one_lookup_returns(engine):
    add_index(engine, 'assoc', 'assoc_old', 'phenotype,chromosome:position')
    add_index(engine, 'assoc-tmp', 'assoc_new',
              'phenotype,chromosome:position;locus_step=250')

    Index.swap_into(engine, 'assoc-tmp', 'assoc')

    found = Index.lookup(engine, 'assoc', 2)
    assert found.table.name == 'assoc_new'
    assert found.schema.locus_step == 250


def test_nothing_moves_when_the_cutover_fails(engine):
    add_index(engine, 'assoc', 'assoc_old', 'phenotype')
    add_index(engine, 'assoc-tmp', 'assoc_new', 'phenotype')
    add_key(engine, 'assoc', 'a/1.json', version='old')
    add_key(engine, 'assoc-tmp', 'a/1.json', version='new')

    before_indexes, before_keys = indexes(engine), keys(engine)

    # fail the __Indexes update, which lands after both __Keys statements
    # have already run: if they are not in the same transaction, the
    # canonical name is left claiming the temp index's keys against its own
    # old table - every one of them pointing into the wrong file
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TRIGGER swap_boom BEFORE UPDATE ON `__Indexes` "
            "FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'boom'"))

    try:
        with pytest.raises(sqlalchemy.exc.DatabaseError):
            Index.swap_into(engine, 'assoc-tmp', 'assoc')
    finally:
        with engine.begin() as conn:
            conn.execute(text('DROP TRIGGER IF EXISTS swap_boom'))

    assert indexes(engine) == before_indexes
    assert keys(engine) == before_keys


def test_swapping_a_name_into_itself_is_refused(engine):
    add_index(engine, 'assoc', 'assoc_live', 'phenotype')
    add_key(engine, 'assoc', 'a/1.json')

    before_indexes, before_keys = indexes(engine), keys(engine)

    with pytest.raises(ValueError, match='into itself'):
        Index.swap_into(engine, 'assoc', 'assoc')

    # unguarded this empties both tables and hands back `assoc_live` - the
    # table the name is still serving - for the caller to drop
    assert indexes(engine) == before_indexes
    assert keys(engine) == before_keys


def test_an_index_left_at_the_zero_date_does_not_count_as_built(engine):
    # the filtered listing feeds GraphQL schema building, which would
    # otherwise raise a type over a table that was never populated
    add_index(engine, 'assoc', 'assoc_t', 'phenotype')
    add_index(engine, 'other', 'other_t', 'phenotype')

    with engine.begin() as conn:
        conn.execute(text('UPDATE `__Indexes` SET `built` = 0 WHERE `name` = :n'),
                     {'n': 'assoc'})

    listed = [i.name for i in Index.list_indexes(engine, filter_built=True)]

    assert listed == ['other']


def test_a_multi_arity_name_is_refused(engine):
    # both of these are legal rows: __Indexes is unique on (name, arity),
    # not on name
    add_index(engine, 'gene', 'genes_1', 'name')
    add_index(engine, 'gene', 'genes_2', 'name,build')
    add_index(engine, 'gene-tmp', 'genes_tmp', 'name')

    before = indexes(engine)

    with pytest.raises(ValueError, match='more than one arity'):
        Index.swap_into(engine, 'gene-tmp', 'gene')

    assert indexes(engine) == before


def test_an_unbuilt_temp_index_is_refused(engine):
    add_index(engine, 'assoc', 'assoc_old', 'phenotype')
    add_index(engine, 'assoc-tmp', 'assoc_new', 'phenotype', built=None)

    with pytest.raises(ValueError, match='never been built'):
        Index.swap_into(engine, 'assoc-tmp', 'assoc')


def test_a_temp_index_left_at_the_zero_date_is_refused(engine):
    add_index(engine, 'assoc', 'assoc_old', 'phenotype')
    add_index(engine, 'assoc-tmp', 'assoc_new', 'phenotype')

    # what create() used to leave behind. MySQL stores it, and pymysql hands
    # it back as a string rather than None, so it reads as built.
    with engine.begin() as conn:
        conn.execute(text(
            'UPDATE `__Indexes` SET `built` = 0 WHERE `name` = :n'), {'n': 'assoc-tmp'})

    with pytest.raises(ValueError, match='never been built'):
        Index.swap_into(engine, 'assoc-tmp', 'assoc')


def test_swapping_in_a_different_arity_is_refused(engine):
    add_index(engine, 'gene', 'genes', 'name')
    add_index(engine, 'gene-tmp', 'genes_tmp', 'name,build')

    with pytest.raises(ValueError, match='query argument'):
        Index.swap_into(engine, 'gene-tmp', 'gene')


def test_a_missing_index_is_refused(engine):
    add_index(engine, 'assoc', 'assoc_old', 'phenotype')

    with pytest.raises(KeyError):
        Index.swap_into(engine, 'assoc-tmp', 'assoc')


def test_recreating_an_index_leaves_it_unbuilt(engine):
    add_index(engine, 'assoc', 'assoc_old', 'phenotype')

    Index.create(engine, 'assoc', 'assoc_new', 'assoc/', 'phenotype')

    with engine.connect() as conn:
        built = conn.execute(text(
            'SELECT `built` FROM `__Indexes` WHERE `name` = :n'), {'n': 'assoc'}).scalar()

    # not the zero date: an index whose table was just repointed has no rows
    # in it, and `list` and swap_into both have to be able to see that
    assert built is None
