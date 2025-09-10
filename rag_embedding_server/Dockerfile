FROM python:3.10-slim

WORKDIR /app

COPY .env-docker /app/.env
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY ./scripts/start-rag-embedding-server.sh /app/script/start-rag-embedding-server.sh
RUN chmod +x /app/script/start-rag-embedding-server.sh

COPY . .

CMD ["bash", "/app/script/start-rag-embedding-server.sh"]