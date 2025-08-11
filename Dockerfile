# base image
FROM python:3.10-slim

ENV PYTHONPATH=/opt/airflow/collector

# system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    python3-dev \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 환경 변수 설정
ENV AIRFLOW_HOME=/opt/airflow

# 작업 디렉토리 설정
WORKDIR ${AIRFLOW_HOME}

# requirements 복사
COPY requirements.txt .

# pip 업그레이드 및 패키지 설치
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# DAGs, plugins 등 필요한 코드 복사
COPY . .

# 포트 오픈
EXPOSE 8080

# 기본 명령
#CMD ["airflow", "webserver"]
