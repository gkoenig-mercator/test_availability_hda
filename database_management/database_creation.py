from sqlalchemy import (
    Table, Column, String, Boolean, Integer, Float, MetaData, DateTime, ForeignKey, Text, create_engine, text
)
import uuid
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

username = os.environ["DATABASE_USERNAME"]
password = os.environ["DATABASE_PASSWORD"]
database_url = os.environ.get(
    "DATABASE_URL", "postgresql-850370.project-test-availability-edito"
)
database_name = os.environ.get("DATABASE_NAME", "defaultdb")

engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{database_url}:5432/{database_name}"
)

metadata = MetaData(schema="testing")

# --- 1. Testing metadata ---
testing_metadata = Table(
    "test_runs",
    metadata,
    Column("id", String, primary_key=True, default=lambda: str(uuid.uuid4())),
    Column("start_time", DateTime, default=datetime.utcnow),
    Column("end_time", DateTime),
    Column("run_duration_seconds", Integer),
    Column("numbers_of_datasets", Integer),
    Column("linux_version", String),
    Column("hda_version", String),
    Column("script_version", String),
)

# --- 2. Datasets tested ---
datasets_tested = Table(
    "test_run_datasets",
    metadata,
    Column("id", String, primary_key=True, default=lambda: str(uuid.uuid4())),
    Column("test_id", String, ForeignKey("test_runs.id")),
    Column("Dataset_id", String),
    Column("Available", Boolean),
    Column("Error", String),
    Column("Min_Lon", Float),
    Column("Max_Lon", Float),
    Column("Min_Lat", Float),
    Column("Max_Lat", Float),
    Column("Start", DateTime),
    Column("End", DateTime),
    Column("Volume", Integer),
    Column("Query", String),
)

def create_schema(engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS testing"))
    metadata.create_all(engine)

if __name__ == "__main__":
    create_schema(engine)