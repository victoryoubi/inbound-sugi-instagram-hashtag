FROM python:3.11-slim

WORKDIR /app

# 依存
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリ本体
COPY src/ ./src/
COPY sql/ ./sql/
COPY configs/ ./configs/

# Cloud Run Jobs は ENTRYPOINT で実行コマンドを受け取れるようにする
ENTRYPOINT ["python", "src/runner.py"]
