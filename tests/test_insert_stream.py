from unittest.mock import MagicMock
from bioindex.lib.index import Index


def _index_capturing_csv(monkeypatch):
    """An Index whose _load_csv records the CSV file contents instead of touching a DB."""
    idx = Index.__new__(Index)
    idx.table = MagicMock(name='t')
    captured = {}

    def fake_load_csv(_self, _engine, infile_name, quoted_fieldnames):
        # patched onto the class, so receives self as the first argument
        with open(infile_name, 'r') as fh:
            captured['csv'] = fh.read()
        captured['fields'] = list(quoted_fieldnames)

    monkeypatch.setattr(Index, '_load_csv', fake_load_csv, raising=False)
    return idx, captured


def test_insert_records_iter_matches_list_path(monkeypatch):
    rows = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]

    idx, cap_list = _index_capturing_csv(monkeypatch)
    idx.insert_records(MagicMock(), list(rows))
    list_csv = cap_list['csv']

    idx2, cap_iter = _index_capturing_csv(monkeypatch)
    idx2.insert_records_iter(MagicMock(), iter(rows))
    iter_csv = cap_iter['csv']

    assert iter_csv == list_csv
    assert 'a,b' in list_csv.splitlines()[0]


def test_insert_records_iter_empty_is_noop(monkeypatch):
    idx, cap = _index_capturing_csv(monkeypatch)
    idx.insert_records_iter(MagicMock(), iter([]))
    assert 'csv' not in cap  # nothing loaded
