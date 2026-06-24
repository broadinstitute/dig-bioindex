import pytest

from bioindex.lib.locus import parse_chromosome


def test_parse_chromosome_accepts_numeric_int():
    # Source JSON often carries chromosome as a number, not a string.
    assert parse_chromosome(1) == "1"
    assert parse_chromosome(22) == "22"


def test_parse_chromosome_accepts_string():
    assert parse_chromosome("1") == "1"
    assert parse_chromosome("chr1") == "1"
    assert parse_chromosome("X") == "X"
    assert parse_chromosome("chrMT") == "MT"


def test_parse_chromosome_rejects_invalid():
    with pytest.raises(ValueError):
        parse_chromosome("banana")
    with pytest.raises(ValueError):
        parse_chromosome(23)
