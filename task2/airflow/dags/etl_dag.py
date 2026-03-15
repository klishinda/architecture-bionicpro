from datetime import date, datetime

import psycopg2
from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.python import PythonOperator

CRM_CONN = {
    "host": "crm_db",
    "port": 5432,
    "dbname": "crm_db",
    "user": "crm_user",
    "password": "crm_password",
}

TELEMETRY_CONN = {
    "host": "telemetry_db",
    "port": 5432,
    "dbname": "telemetry_db",
    "user": "telemetry_user",
    "password": "telemetry_password",
}

CLICKHOUSE_HOST = "clickhouse"


def extract_crm(**context):
    conn = psycopg2.connect(**CRM_CONN)
    cur = conn.cursor()
    cur.execute("SELECT username, prosthesis_serial FROM patients")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    # {prosthesis_serial: username}
    mapping = {row[1]: row[0] for row in rows}
    context["ti"].xcom_push(key="crm_mapping", value=mapping)


def extract_telemetry(**context):
    execution_date = context["ds"]  # YYYY-MM-DD string
    conn = psycopg2.connect(**TELEMETRY_CONN)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT prosthesis_serial, recorded_date::text, avg_response_ms, movement_count
        FROM telemetry
        WHERE recorded_date = %s
        """,
        (execution_date,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    context["ti"].xcom_push(key="telemetry_rows", value=rows)


def transform_and_load(**context):
    ti = context["ti"]
    crm_mapping = ti.xcom_pull(key="crm_mapping", task_ids="extract_crm")
    telemetry_rows = ti.xcom_pull(key="telemetry_rows", task_ids="extract_telemetry")

    if not telemetry_rows:
        print("No telemetry data for this date — skipping load.")
        return

    processed_at = datetime.utcnow()
    records = []
    for prosthesis_serial, recorded_date, avg_response_ms, movement_count in telemetry_rows:
        username = crm_mapping.get(prosthesis_serial)
        if username is None:
            continue
        records.append(
            (username, prosthesis_serial, date.fromisoformat(recorded_date), avg_response_ms, movement_count, processed_at)
        )

    if not records:
        print("No matching CRM records found — skipping load.")
        return

    client = Client(host=CLICKHOUSE_HOST)
    client.execute(
        """
        INSERT INTO reports.user_reports
            (username, prosthesis_serial, report_date, avg_response_ms, movement_count, processed_at)
        VALUES
        """,
        records,
    )
    print(f"Loaded {len(records)} records into ClickHouse.")


with DAG(
    dag_id="etl_reports",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bionic"],
) as dag:
    t1 = PythonOperator(task_id="extract_crm", python_callable=extract_crm)
    t2 = PythonOperator(task_id="extract_telemetry", python_callable=extract_telemetry)
    t3 = PythonOperator(task_id="transform_and_load", python_callable=transform_and_load)

    [t1, t2] >> t3
