from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago
import pathlib

NAMESPACE = "airflow"
RESOURCES_DIR = pathlib.Path("/opt/airflow/dags/resources")

# (task name, SparkApplication manifest file), in pipeline order
JOBS = [
    ("ingest-raw-data", "ingest-raw-data-job.yaml"),
    ("validate-raw-data", "validate-raw-data-job.yaml"),
    ("compute-metrics", "compute-metrics-job.yaml"),
    ("load-metrics", "load-metrics-job.yaml"),
]

default_args = {
    "owner": "Nader Hachana",
    "start_date": days_ago(0),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_pipeline_automation",
    default_args=default_args,
    description="Ingests retail sales data, validates it, computes sales metrics, and loads them back into Cassandra",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    previous_task = None
    for task_name, manifest_file in JOBS:
        submit = SparkKubernetesOperator(
            task_id=task_name,
            namespace=NAMESPACE,
            application_file=(RESOURCES_DIR / manifest_file).read_text(),
            kubernetes_conn_id="kubernetes_default",
            do_xcom_push=True,
        )
        wait_for_completion = SparkKubernetesSensor(
            task_id=f"{task_name}-status",
            namespace=NAMESPACE,
            application_name="{{ task_instance.xcom_pull(task_ids='%s')['metadata']['name'] }}" % task_name,
            kubernetes_conn_id="kubernetes_default",
            attach_log=True,
        )
        submit >> wait_for_completion
        if previous_task is not None:
            previous_task >> submit
        previous_task = wait_for_completion
