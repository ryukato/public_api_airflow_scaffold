# Unit tests for record_parser (comments in English)
import pytest
import collector.parser.record_parser as rp

def test_parse_records_success():
    # It should collect all non-None documents returned by the item_parser.
    records = [{"x": 1}, {"x": 2}, {"x": 3}]
    def item_parser(rec):
        return {"y": rec["x"]}
    docs = rp.parse_records(records, item_parser)
    assert docs == [{"y": 1}, {"y": 2}, {"y": 3}]

def test_parse_records_skips_none():
    # It should skip items when the item_parser returns None.
    records = [{"x": 1}, {"x": 2}, {"x": 3}]
    def item_parser(rec):
        return None if rec["x"] % 2 == 0 else {"y": rec["x"]}
    docs = rp.parse_records(records, item_parser)
    assert docs == [{"y": 1}, {"y": 3}]

def test_parse_records_logs_and_continues_on_exception(caplog):
    # It should log an exception for a bad record and continue with the rest.
    records = [{"x": 1}, {"x": "bad"}, {"x": 3}]
    def item_parser(rec):
        if rec["x"] == "bad":
            raise ValueError("boom")
        return {"y": rec["x"]}
    with caplog.at_level("ERROR"):
        docs = rp.parse_records(records, item_parser)
    assert docs == [{"y": 1}, {"y": 3}]
    assert any("parse error" in msg.lower() for _, _, msg in caplog.record_tuples)

def test_parse_item_dummy_minimal():
    # parse_item_dummy should return a dict with python field names (not aliases).
    rec = {"ITEM_SEQ": "123", "ITEM_NAME": "Foo", "ENTP_NAME": "Bar"}
    doc = rp.parse_item_dummy(rec)
    assert isinstance(doc, dict)
    assert doc["id"] == "123"
