"""Shared pytest fixtures for all tests."""

import pytest


@pytest.fixture
def sample_jsic_revision_mapping():
    """Sample JSIC revision to release date mapping."""
    return {
        "04": "2023-07-01",
        "03": "2013-10-01",
        "02": "2007-11-01",
        "01": "2002-03-01",
    }
