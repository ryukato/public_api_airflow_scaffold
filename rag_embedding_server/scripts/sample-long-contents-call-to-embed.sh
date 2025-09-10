#!/bin/bash
curl -X POST http://localhost:8000/test/embed \
  -H "Content-Type: application/json" \
  -d '[
        {
            "chunk_id": "$uuid",
            "name": "test",
            "meta": {
                "source_ref": "TEST_SOURCE",
                "lang": "ko"
            }
        }
  ]'
