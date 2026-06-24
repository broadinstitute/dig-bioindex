import os

from bioindex import main


def test_dev_generates_key_when_unset(monkeypatch):
    monkeypatch.delenv("BIOINDEX_TOKEN_SIGNING_KEY", raising=False)
    warn = main._maybe_dev_signing_key(dev=True)
    assert os.environ.get("BIOINDEX_TOKEN_SIGNING_KEY")
    assert warn and "ephemeral" in warn.lower()


def test_dev_keeps_existing_key(monkeypatch):
    monkeypatch.setenv("BIOINDEX_TOKEN_SIGNING_KEY", "ab" * 32)
    warn = main._maybe_dev_signing_key(dev=True)
    assert os.environ["BIOINDEX_TOKEN_SIGNING_KEY"] == "ab" * 32
    assert warn is None


def test_no_dev_no_key_is_noop(monkeypatch):
    monkeypatch.delenv("BIOINDEX_TOKEN_SIGNING_KEY", raising=False)
    warn = main._maybe_dev_signing_key(dev=False)
    assert warn is None
    assert not os.environ.get("BIOINDEX_TOKEN_SIGNING_KEY")


def test_dev_key_is_valid_for_signing(monkeypatch):
    monkeypatch.delenv("BIOINDEX_TOKEN_SIGNING_KEY", raising=False)
    main._maybe_dev_signing_key(dev=True)
    from bioindex.lib import signed_tokens
    signed_tokens.signing_key.cache_clear()
    assert len(signed_tokens.signing_key()) >= 32
