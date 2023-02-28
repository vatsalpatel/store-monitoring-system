import os

from dotenv import load_dotenv
import redis
import psycopg2


load_dotenv()

redis_client = redis.Redis(os.getenv("REDIS_HOST"), os.getenv("REDIS_PORT"))

postgres_client = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    database=os.getenv("POSTGRES_DATABASE"),
)
