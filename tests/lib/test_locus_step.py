import pytest

from bioindex.lib.locus import RegionLocus, SNPLocus, parse_chromosome, parse_locus_builder
from bioindex.lib.schema import Schema


# --- the step reaches the buckets ------------------------------------------

def test_the_default_step_is_unchanged():
    # 45123 // 20000 * 20000
    assert list(SNPLocus("1", 45123).loci()) == [("1", 40000)]
    assert list(RegionLocus("1", 10000, 50000).loci()) == [("1", 0), ("1", 20000), ("1", 40000)]


def test_a_smaller_step_buckets_more_finely():
    assert list(SNPLocus("1", 45123, step=250).loci()) == [("1", 45000)]
    assert list(RegionLocus("1", 45100, 45600, step=250).loci()) == [
        ("1", 45000), ("1", 45250), ("1", 45500),
    ]


@pytest.mark.parametrize("locus_str,args", [
    ("varId=$chr:$pos", ("1:45123:A:G",)),
    ("chromosome:position", ("1", 45123)),
])
def test_a_builder_bakes_the_step_into_what_it_makes(locus_str, args):
    builder, _ = parse_locus_builder(locus_str, step=250)
    assert list(builder(*args).loci()) == [("1", 45000)]

    builder, _ = parse_locus_builder(locus_str)
    assert list(builder(*args).loci()) == [("1", 40000)]


# --- the schema string carries it ------------------------------------------

def test_a_schema_without_the_modifier_keeps_the_default():
    assert Schema("varId=$chr:$pos").locus_step == 20000


@pytest.mark.parametrize("schema_str,step", [
    ("varId=$chr:$pos;locus_step=250", 250),
    ("phenotype,chromosome:start-stop;locus_step=500", 500),
])
def test_the_modifier_sets_the_step(schema_str, step):
    assert Schema(schema_str).locus_step == step


def test_the_modifier_does_not_become_a_column():
    stepped = Schema("phenotype,chromosome:start-stop;locus_step=500")
    plain = Schema("phenotype,chromosome:start-stop")

    # arity is derived from the column count, so a modifier that leaked in
    # would silently change which index a query resolves to
    assert stepped.schema_columns == plain.schema_columns
    assert stepped.key_columns == plain.key_columns


def test_the_step_reaches_the_locus_the_schema_builds():
    schema = Schema("varId=$chr:$pos;locus_step=250")

    assert list(schema.locus_class("1:45123:A:G").loci()) == [("1", 45000)]


@pytest.mark.parametrize("bad", ["varId=$chr:$pos;locus_step=abc", "varId=$chr:$pos;locus_step="])
def test_a_non_integer_step_is_rejected(bad):
    with pytest.raises(ValueError, match="not an integer"):
        Schema(bad)


@pytest.mark.parametrize("bad", ["varId=$chr:$pos;locus_step=0", "varId=$chr:$pos;locus_step=-5"])
def test_a_non_positive_step_is_rejected(bad):
    with pytest.raises(ValueError, match="must be > 0"):
        Schema(bad)


def test_a_step_on_a_schema_with_no_locus_is_rejected():
    with pytest.raises(ValueError, match="without a locus"):
        Schema("phenotype;locus_step=250")


# --- numeric chromosome columns --------------------------------------------

@pytest.mark.parametrize("value,expected", [(8, "8"), ("8", "8"), ("chr8", "8"), ("X", "X")])
def test_a_chromosome_may_arrive_as_a_number(value, expected):
    # a JSON record with a numeric chromosome column yields an int
    assert parse_chromosome(value) == expected
