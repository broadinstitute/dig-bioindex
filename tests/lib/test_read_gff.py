import gzip

import pytest
import smart_open

from bioindex.lib.utils import read_gff

# the real genes file is gzipped and CRLF-terminated, and carries no '##'
# directives, so every line is a full nine columns
ROWS = [
    b"19\t.\tprotein_coding\t58856544\t58864865\t.\t+\t.\tName=A1BG;Alias=ENSG00000121410,HGNC:5\r\n",
    b"10\tensembl\tprotein_coding\t52559169\t52645435\t.\t-\t.\tName=A1CF\r\n",
]


@pytest.fixture(params=["plain", "gzipped"])
def gff(request, tmp_path):
    if request.param == "gzipped":
        path = tmp_path / "genes.gff.gz"
        path.write_bytes(gzip.compress(b"".join(ROWS)))
    else:
        path = tmp_path / "genes.gff"
        path.write_bytes(b"".join(ROWS))
    return str(path)


def test_it_reads_every_column_it_cares_about(gff):
    first, second = list(read_gff(gff))

    assert first[:5] == ("19", None, "protein_coding", 58856544, 58864865)
    assert first[5]["Name"] == "A1BG"
    assert first[5]["Alias"] == "ENSG00000121410,HGNC:5"

    # '.' means absent, and is reported as None rather than a literal dot
    assert second[1] == "ensembl"
    assert first[1] is None


def test_the_carriage_return_does_not_leak_into_the_last_column(gff):
    first, _ = list(read_gff(gff))

    assert not first[5]["Alias"].endswith("\r")


def test_it_does_not_depend_on_the_removed_legacy_alias(gff, monkeypatch):
    # prod runs a smart_open with no `smart_open.smart_open`, and every gene
    # query 500'd on it; the alias still exists locally, so take it away
    monkeypatch.delattr(smart_open, "smart_open", raising=False)

    assert len(list(read_gff(gff))) == 2
