import os
from time import sleep

from dotenv import load_dotenv
from celery import Celery

load_dotenv()

celery = Celery("tasks")
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND")


@celery.task(bind=True)
def generate_report(self):
    return True
