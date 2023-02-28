# store-monitoring-system

### Tech stack
Built with Python, FastAPI, Celery, Redis, and PostgreSQL

### Setup
Run Postgres and Redis in docker or install locally. <br/>
Install python packages using 
```
pip install -r requirements.txt
```
Provide the following in .env file
```
POSTGRES_HOST = ""
POSTGRES_PORT = ""
POSTGRES_USER = ""
POSTGRES_PASSWORD = ""
POSTGRES_DATABASE = ""
REDIS_HOST = ""
REDIS_PORT = ""
CELERY_BROKER_URL = ""
CELERY_RESULT_BACKEND = ""
```

### Usage
Make POST request to `/trigger_report` to receive report id. <br/>
Make POST request to `"/get_report"` with report id to receive the report.

### Database Schema
```
CREATE TABLE store_status (
    store_id VARCHAR(50),
    status VARCHAR(50),
    timestamp_utc TIMESTAMP
);

CREATE TABLE menu_hours (
    store_id VARCHAR(50),
    day VARCHAR(50),
    start_time_local VARCHAR(50),
    end_time_local VARCHAR(50)
);

CREATE TABLE store_timezone (
    store_id VARCHAR(50),
    timezone_str VARCHAR(50)
);

CREATE TABLE generated_reports (
    report_id VARCHAR(50),
    csv TEXT,
    created_at TIMESTAMP
);
```
