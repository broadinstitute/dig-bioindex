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
