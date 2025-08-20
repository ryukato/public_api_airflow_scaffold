# Smoke test for MongoCollectionAdapter (comments in English)
import collector.repository.mongo_collection_adapter as adapter_mod

def test_adapter_calls_repo(monkeypatch):
    called = {}
    class DummyRepo:
        def upsert_many(self, docs, key_fields, batch_size=None):
            called["args"] = (list(docs), key_fields, batch_size)
            return 42
    adapter = adapter_mod.MongoCollectionAdapter.__new__(adapter_mod.MongoCollectionAdapter)
    adapter.MODEL_CFG = {"dataset_a": ("dataset_a_raw", ("id",))}
    adapter._rw_repo = lambda col: DummyRepo()
    n = adapter.dataset_a_upsert_many([{"id": "A"}], batch_size=123)
    assert n == 42
    assert called["args"][0] == [{"id": "A"}]
    assert called["args"][1] == ("id",)
    assert called["args"][2] == 123
