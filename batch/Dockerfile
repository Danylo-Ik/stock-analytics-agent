FROM bitnamilegacy/spark:3.5.1

USER root

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

COPY scheduler.py .

USER 1001

CMD ["python3", "scheduler.py"]