#!/bin/bash
COLLECTION_NAME="test"
curl -X POST "http://localhost:6333/collections/${COLLECTION_NAME}/points/count" \                                                                                                                                                                                        ok | 3.10.14 py | at 16:13:32
     -H "Content-Type: application/json" \
     -d '{"exact": true}'
