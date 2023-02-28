import os

from dotenv import load_dotenv
from celery import Celery

from db import redis_client, postgres_client
from report import generate_report


load_dotenv()

celery = Celery("tasks")
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND")


@celery.task(bind=True)
def generate_report_task(self):
    report_id = self.request.id
    redis_client.set(report_id, "RUNNING")

    # Generate and store report here
    report = generate_report()
    report_to_save = "\n".join(report)
    cursor = postgres_client.cursor()
    cursor.execute(f"INSERT INTO generated_reports VALUES ('{report_id}', '{report_to_save}')")
    postgres_client.commit()

    # Set status in redis and return to handler
    redis_client.set(report_id, "COMPLETE")
    return True
