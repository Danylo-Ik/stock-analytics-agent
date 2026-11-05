"""
B1 hourly_report - every hour (start of the hour)
B2 trading_patterns — every hour (30 min after hour)
B3 large_trades — every 4 hours
"""

import logging
import os
import subprocess
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("batch-scheduler")

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")

PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
)

BASE_CMD = [
    "spark-submit",
    "--master", SPARK_MASTER,
    "--packages", PACKAGES,
    "--conf", f"spark.cassandra.connection.host={CASSANDRA_HOST}",
    "--conf", "spark.sql.shuffle.partitions=4",
    "--driver-memory", "512m",
    "--executor-memory", "1g",
]


def run_job(script_path: str, job_name: str):
    cmd = BASE_CMD + [script_path]
    logger.info("Starting batch job: %s", job_name)
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        logger.info("Batch job %s finished (rc=%d)", job_name, result.returncode)
    except subprocess.CalledProcessError as exc:
        logger.error("Batch job %s failed (rc=%d)", job_name, exc.returncode)
    except Exception as exc:
        logger.error("Unexpected error in %s: %s", job_name, exc)


def main():
    logger.info("Waiting 60s before starting batch scheduler...")
    time.sleep(60)

    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        run_job,
        CronTrigger.from_crontab(os.getenv("HOURLY_REPORT_CRON", "0 * * * *")),
        args=["/app/src/hourly_report.py", "HourlyReport"],
        id="hourly_report",
        name="Hourly Trading Report",
        max_instances=1,
        misfire_grace_time=300,
    )

    scheduler.add_job(
        run_job,
        CronTrigger.from_crontab(os.getenv("TRADING_PATTERNS_CRON", "30 * * * *")),
        args=["/app/src/trading_patterns.py", "TradingPatterns"],
        id="trading_patterns",
        name="Trading Patterns Analysis",
        max_instances=1,
        misfire_grace_time=300,
    )

    scheduler.add_job(
        run_job,
        CronTrigger.from_crontab(os.getenv("LARGE_TRADES_CRON", "0 */4 * * *")),
        args=["/app/src/large_trades_analysis.py", "LargeTradesAnalysis"],
        id="large_trades",
        name="Large Trades Analysis",
        max_instances=1,
        misfire_grace_time=600,
    )

    logger.info("Batch scheduler started - jobs: %s",
                [j.name for j in scheduler.get_jobs()])
    scheduler.start()


if __name__ == "__main__":
    main()
