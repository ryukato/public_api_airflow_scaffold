#!/bin/bash

# === 설정 ===
IMAGE_NAME="rag-embedding-server"
TAG="latest"
DOCKERFILE_PATH="Dockerfile"
CONTEXT_DIR="."

# === 빌드 시작 ===
echo "🚀 Docker 이미지 빌드 시작: ${IMAGE_NAME}:${TAG}"
docker build -f $DOCKERFILE_PATH -t ${IMAGE_NAME}:${TAG} $CONTEXT_DIR

# === 결과 확인 ===
if [ $? -eq 0 ]; then
    echo "✅ Docker 이미지 빌드 완료: ${IMAGE_NAME}:${TAG}"
else
    echo "❌ Docker 이미지 빌드 실패"
    exit 1
fi