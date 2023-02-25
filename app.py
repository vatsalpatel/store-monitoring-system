from dotenv import load_dotenv
from fastapi import FastAPI, Request
from celery.result import AsyncResult

from worker import generate_report

load_dotenv()
app = FastAPI()


@app.post("/trigger_report")
async def triggerReport():
    task = generate_report.delay()
    print(task.task_id)
    return {"reportId": task.task_id}


@app.post("/get_report")
async def getReport(request: Request):
    body = await request.json()
    report_id = body.get("reportId")
    res = AsyncResult(report_id)
    print(report_id, res.ready())
    return {"report": report_id, "state": res.ready()}
