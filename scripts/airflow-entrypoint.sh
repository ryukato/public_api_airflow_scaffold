#!/usr/bin/env bash

# migrate는 최초 실행일 경우만
if [ ! -f "/opt/airflow/airflow.db_migrated" ]; then
  echo "🔧 Initializing Airflow DB..."
  airflow db migrate && touch /opt/airflow/airflow.db_migrated
else
  echo "✅ Airflow DB already migrated."
fi

echo "🚀 Starting Airflow with: $@"
exec "$@"