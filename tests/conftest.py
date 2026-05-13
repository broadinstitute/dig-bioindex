import pytest


@pytest.fixture
def sample_portal_dict():
    """Minimal portal config dict for tests that need one."""
    return {
        "BIOINDEX_S3_BUCKET": "test-bucket",
        "BIOINDEX_RDS_INSTANCE": "test-rds",
        "BIOINDEX_RDS_USERNAME": "u",
        "BIOINDEX_RDS_PASSWORD": "p",
        "BIOINDEX_BIO_SCHEMA": "test_schema",
    }


@pytest.fixture(autouse=True)
def _reset_registry():
    """
    Reset the process-global PortalRegistry singleton before and after every
    test so registry state doesn't leak between tests.
    """
    import bioindex.lib.portal_registry as pr
    pr._registry = None
    yield
    pr._registry = None
