#!/bin/bash

# Usage: ./sample-batch-contents-call-to-embed.sh [batch_size]
# Default batch size is 100
COLLECTION_NAME="test"
BATCH_SIZE=${1:-100}  # use first argument or default to 100
URL="http://localhost:8000/${COLLECTION_NAME}/embed"

echo "Sending $BATCH_SIZE chunks to $URL"

payload='['

for ((i = 1; i <= BATCH_SIZE; i++)); do
  uuid=$(uuidgen)
  content="test"

  chunk=$(cat <<EOF
{
  "chunk_id": "$uuid",
  "name": "test",
  "meta": {
    "source_ref": "TEST_SOURCE",
    "lang": "ko"
  }
}
EOF
)
  payload+="$chunk"
  if [[ $i -lt $BATCH_SIZE ]]; then
    payload+=","
  fi
done

payload+=']'

curl -X POST "$URL" \
     -H "Content-Type: application/json" \
     -d "$payload"
