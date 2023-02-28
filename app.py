from dotenv import load_dotenv
from fastapi import FastAPI, Request
from celery.result import AsyncResult

from worker import generate_report_task
from db import postgres_client

load_dotenv()
app = FastAPI()


@app.post("/trigger_report")
async def triggerReport():
    task = generate_report_task.delay()
    return {"reportId": task.task_id}


@app.post("/get_report")
async def getReport(request: Request):
    body = await request.json()
    report_id = body.get("reportId")

    res = AsyncResult(report_id)
    complete = res.ready()

    if complete:
        if res.result is not True:
            return {"reportId": report_id, "status": "Complete", "success": False}
        cursor = postgres_client.cursor()
        cursor.execute(f"SELECT * FROM generated_reports WHERE report_id = '{report_id}'")
        report = cursor.fetchone()[1]
        return {"reportId": report_id, "status": "Complete", "report": report, "success": True}

    return {"reportId": report_id, "status": "Running", "success": True}
