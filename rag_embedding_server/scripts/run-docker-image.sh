#!/bin/bash

# === 설정 ===
IMAGE_NAME="rag-embedding-server"
TAG="latest"
CONTAINER_NAME="rag-embedding-server"
HOST_PORT=8000
CONTAINER_PORT=8000

# 필요시 환경변수 설정
ENV_FILE=".env"

# === 실행 중인 컨테이너가 있으면 중지 및 삭제 ===
if [ "$(docker ps -aq -f name=^/${CONTAINER_NAME}$)" ]; then
  echo "🛑 기존 컨테이너 정리 중: ${CONTAINER_NAME}"
  docker stop ${CONTAINER_NAME}
  docker rm ${CONTAINER_NAME}
fi

# === 컨테이너 실행 ===
echo "🚀 Docker 컨테이너 실행: ${CONTAINER_NAME}"
docker run -d \
  --name ${CONTAINER_NAME} \
  -p ${HOST_PORT}:${CONTAINER_PORT} \
  --env-file ${ENV_FILE} \
  ${IMAGE_NAME}:${TAG}

# === 결과 확인 ===
if [ $? -eq 0 ]; then
  echo "✅ 컨테이너가 정상적으로 시작되었습니다. → http://localhost:${HOST_PORT}"
else
  echo "❌ 컨테이너 실행 실패"
  exit 1
fi