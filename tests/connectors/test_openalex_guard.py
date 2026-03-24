import pytest
from connectors.api.openalex import OpenAlexConnector

def test_max_records_none_becomes_inf():
    conn = OpenAlexConnector(max_records=None)
    assert conn.max_records == float("inf")

def test_max_records_int_unchanged():
    conn = OpenAlexConnector(max_records=200)
    assert conn.max_records == 200

def test_max_records_default_unchanged():
    conn = OpenAlexConnector()
    assert conn.max_records == 500
