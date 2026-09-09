# 2.5.1 has a confirmed upstream bug (apache/airflow#29841): importing
# python-daemon causes unbounded memory growth on Linux, so the webserver
# consumes all available memory and gets OOMKilled before it can even start.
# Fixed in 2.5.2.
FROM apache/airflow:2.5.2-python3.10

# Build context is the repo root (see k8s/docker-compose.yml) since the DAG
# and the manifests it bakes in live in separate top-level directories.

# Copying dags to airflow dags folder
COPY ./airflow/dags/batch-pipeline.py /opt/airflow/dags

# Copying spark apps manifest files to resources folder in airflow
COPY ./k8s/manifests/spark-apps/ingest-raw-data-job.yaml /opt/airflow/dags/resources/ingest-raw-data-job.yaml

COPY ./k8s/manifests/spark-apps/validate-raw-data-job.yaml /opt/airflow/dags/resources/validate-raw-data-job.yaml

COPY ./k8s/manifests/spark-apps/compute-metrics-job.yaml /opt/airflow/dags/resources/compute-metrics-job.yaml

COPY ./k8s/manifests/spark-apps/load-metrics-job.yaml /opt/airflow/dags/resources/load-metrics-job.yaml
