FROM bitnamilegacy/spark:3.5.1

USER root

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

COPY run_jobs.sh .
RUN chmod +x run_jobs.sh

USER 1001

CMD ["tail", "-f", "/dev/null"]