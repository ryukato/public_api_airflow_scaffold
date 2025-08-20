source $PWD/.venv/bin/activate
# config python path
export PYTHONPATH=$PWD/collector

# override airflow config
export AIRFLOW_ROOT=$PWD
export AIRFLOW_HOME=$AIRFLOW_ROOT/airflow_home
export AIRFLOW_CONFIG="$AIRFLOW_HOME/airflow.cfg"
export AIRFLOW__WEBSERVER__CONFIG_FILE="$AIRFLOW_HOME/webserver_config.py"
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_ROOT/drug_collector/collector/dag"
export AIRFLOW__CORE__PLUGINS_FOLDER="$AIRFLOW_HOME/plugins"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$AIRFLOW_HOME/logs"
export AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION="$AIRFLOW_HOME/logs/dag_processor_manager/dag_processor_manager.log"
export AIRFLOW__SCHEDULER__CHILD_PROCESS_LOG_DIRECTORY="$AIRFLOW_HOME/logs/scheduler"
echo "AIRFLOW_HOME=${AIRFLOW_HOME}"

# Airflow 실행 설정
# change executore depends on your environment, but SequentialExecutor is recommended on local environment.
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__WEBSERVER__WORKERS=1
unset AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT  # 선택: 디버그 아니면 기본으로

# 기타 타임아웃은 유지 가능
export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=600
export AIRFLOW__SCHEDULER__DAG_FILE_PROCESSOR_TIMEOUT=600
export AIRFLOW__SCHEDULER__ZOMBIE_TASK_THRESHOLD=600
export AIRFLOW__SCHEDULER__JOB_HEARTBEAT_SEC=5
export AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=5
export AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL=5
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=60
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30

# HF 캐시 설정은 그대로 OK
export HF_HOME="$AIRFLOW_HOME/.hf_cache"
export HUGGINGFACE_HUB_CACHE="$HF_HOME/hub"
export SENTENCE_TRANSFORMERS_HOME="$HF_HOME/sentence_transformers"
export TORCH_HOME=$HF_HOME/torch
export TRANSFORMERS_OFFLINE=1
export HF_HUB_OFFLINE=0
export TOKENIZERS_PARALLELISM=false
export HF_HUB_DISABLE_TELEMETRY=1

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export RAG_EMBED_DEVICE=cpu
export RAG_EMBED_SKIP_MPS_CHECK=1

# To fix issue from requests
export NO_PROXY="*"

# run scheduler
airflow scheduler
