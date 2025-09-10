#!/bin/bash
COLLECTION_NAME="test"
curl -X POST "http://localhost:6333/collections/${COLLECTION_NAME}/points/scroll" \                                                                                                                                                                                      INT | 3.10.14 py | at 16:14:34
  -H "Content-Type: application/json" \
  -d '{
    "limit": 10, "with_vector": true
  }'