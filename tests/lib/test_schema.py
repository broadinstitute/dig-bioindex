import pytest

from bioindex.lib.locus import Locus
from bioindex.lib.schema import Schema


def test_default_locus_step_is_20000():
    s = Schema("varId=$chr:$pos")
    assert s.locus_step == Locus.LOCUS_STEP == 20000


def test_parses_suffix_template():
    s = Schema("varId=$chr:$pos;locus_step=250")
    assert s.locus_step == 250
    assert s.has_locus
    assert s.schema_str == "varId=$chr:$pos;locus_step=250"


def test_parses_suffix_column():
    assert Schema("chromosome:start-stop;locus_step=500").locus_step == 500


def test_parses_suffix_compound_keeps_arity():
    s = Schema("phenotype,chromosome:start-stop;locus_step=500")
    assert s.locus_step == 500
    assert s.key_columns == ["phenotype"]
    assert s.arity == 2


def test_rejects_non_integer_step():
    with pytest.raises(ValueError):
        Schema("varId=$chr:$pos;locus_step=abc")


def test_rejects_nonpositive_step():
    with pytest.raises(ValueError):
        Schema("varId=$chr:$pos;locus_step=0")


def test_rejects_step_without_locus():
    with pytest.raises(ValueError):
        Schema("phenotype;locus_step=250")


def test_index_builder_uses_locus_step():
    # build-time bucketing: a varId at pos 45123 buckets to 45000 at step 250
    s = Schema("varId=$chr:$pos;locus_step=250")
    assert list(s.index_builder({"varId": "1:45123:A:G"})) == [("1", 45000)]


def test_index_builder_default_step():
    s = Schema("varId=$chr:$pos")
    assert list(s.index_builder({"varId": "1:45123:A:G"})) == [("1", 40000)]
