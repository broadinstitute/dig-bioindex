from unittest.mock import MagicMock
from click.testing import CliRunner

from bioindex import main


def test_swap_cli_calls_swap_then_drops_old_table(monkeypatch):
    fake_engine = MagicMock()
    monkeypatch.setattr(main.config, "Config", lambda: MagicMock())
    monkeypatch.setattr(main.migrate, "migrate", lambda cfg: fake_engine)
    monkeypatch.setattr(main.index.Index, "swap_into",
                        staticmethod(lambda engine, t, c: "OLD_TABLE"))

    executed = {}

    class _Conn:
        def execute(self, statement):
            executed['sql'] = str(statement)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    fake_engine.begin.return_value = _Conn()

    res = CliRunner().invoke(main.cli, ["swap", "tmp", "canon"], input="y\n", obj={})

    assert res.exit_code == 0, res.output
    assert "DROP TABLE" in executed['sql'].upper()
    assert "OLD_TABLE" in executed['sql']
