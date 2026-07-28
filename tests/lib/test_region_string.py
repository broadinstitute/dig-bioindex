import locale
import types

import pytest

from bioindex.lib.locus import RegionLocus, parse_region_string


@pytest.fixture
def config():
    return types.SimpleNamespace(genes_dict={"ABC": RegionLocus("8", 100, 200)})


@pytest.mark.parametrize("region,expected", [
    ("chr8:118184783-118184793", ("8", 118184783, 118184793)),
    ("8:118184783-118184793", ("8", 118184783, 118184793)),
    ("X:1000-2000", ("X", 1000, 2000)),
    # a bare position spans a single base
    ("1:500", ("1", 500, 501)),
    # '+' makes the tail a length, '/' makes it a radius around the start
    ("1:1000+50", ("1", 1000, 1050)),
    ("1:1000/50", ("1", 950, 1051)),
])
def test_region_forms(config, region, expected):
    assert parse_region_string(region, config) == expected


@pytest.mark.parametrize("region,expected", [
    ("8:118,184,783-118,184,793", ("8", 118184783, 118184793)),
    ("1:1,000+2,500", ("1", 1000, 3500)),
    ("1:1,000/500", ("1", 500, 1501)),
])
def test_thousands_separators_are_accepted(config, region, expected):
    assert parse_region_string(region, config) == expected


def test_parsing_never_touches_the_process_locale(config, monkeypatch):
    # it used to set LC_ALL to en_US.UTF8 per call, which the slim base image
    # cannot do at all, and which is process-global under a threaded server
    def fail(*args, **kwargs):
        raise AssertionError("parse_region_string must not set the locale")

    monkeypatch.setattr(locale, "setlocale", fail)

    assert parse_region_string("8:118,184,783-118,184,793", config) == ("8", 118184783, 118184793)


def test_an_unparseable_region_falls_back_to_the_gene_dictionary(config):
    assert parse_region_string("abc", config) == ("8", 100, 200)


def test_an_unknown_gene_is_an_error(config):
    with pytest.raises(ValueError, match="no such gene"):
        parse_region_string("NOTAGENE", config)


@pytest.mark.parametrize("region", ["1:2000-1000", "1:1000-1000"])
def test_a_stop_at_or_before_the_start_is_an_error(config, region):
    with pytest.raises(ValueError, match="must be > start"):
        parse_region_string(region, config)
