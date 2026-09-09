# Retail Sales Pipeline

A batch data pipeline for retail sales: ingest, validate, compute sales metrics, load. Built with Scala/Spark and Cassandra, orchestrated with Airflow, deployable on Docker Compose for local development or Kubernetes for something closer to production.

Runs against a real, public dataset: [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii), about 1.07M real transactions from a UK-based online retailer.

[![CI](https://github.com/nader-hachana/retail-sales-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/nader-hachana/retail-sales-pipeline/actions/workflows/ci.yml)

## Pipeline

Four Spark jobs, run in sequence:

1. **ingest_raw_data**: loads the 3 raw CSVs (sales, products, calendar) into Cassandra.
2. **validate_raw_data**: checks each table for missing/incorrect values. Non-critical gaps just warn (e.g. a missing product description); critical ones (e.g. a missing price) fail the pipeline.
3. **compute_metrics**: computes weekly sales, a promo-price-drop proxy, baseline and lift, a naive trailing-average forecast, and per-sku price history.
4. **load_metrics**: writes those metrics back into Cassandra.

```
raw CSVs -> [ingest_raw_data] -> Cassandra (sales.*)
                                       |
                              [validate_raw_data]
                                       |
                              [compute_metrics] -> parquet
                                       |
                               [load_metrics] -> Cassandra (sales_metrics.*)
```

`jobs/common` holds the code shared across jobs (Spark session setup, Cassandra reads, test fixtures). Each job stays independently buildable and deployable, since in production they'd retry and scale separately.

## Design notes

A few decisions worth knowing about if you're reading the code:

- **The promo flag is a proxy, not real data.** This dataset has no promotion field, so `compute_metrics` flags a sale as promotional when its price is meaningfully below the sku's own median price (not its all-time max, which real data showed was itself an outlier for most skus).
- **Cancellations are netted, not dropped.** A cancelled order and its original sale are both kept and summed per week, so a large order that was immediately cancelled doesn't inflate that week's numbers (this is a real case in the dataset: sku `23843`, a ~81,000-unit order cancelled the same day).
- **The forecast is a naive trailing 4-week average.** There's no legitimate external forecast source for this data, so this is a deliberately simple baseline, not a claim of a good model.

## Quick start (Docker Compose)

Needs Docker and Python 3 with `pandas` installed (or run it with `uv`, no setup needed: `uv run --with pandas python3 scripts/fetch_data.py`).

```bash
python3 scripts/fetch_data.py
docker compose up --build
```

This downloads the real dataset into `data/raw/`, then runs all 4 jobs against a local Cassandra. Takes a few minutes; `compute_metrics` is the slowest step (full dataset, several aggregations).

Check the results:

```bash
docker exec bpa-cassandra cqlsh -e "SELECT * FROM sales_metrics.weekly_sales LIMIT 5"
```

## Kubernetes

The same pipeline, orchestrated by Airflow on Kubernetes via the [Spark Operator](https://github.com/kubeflow/spark-operator). Needs `kind`, `helm`, and `kubectl` (`brew install kind helm kubectl` on macOS).

```bash
# 1. cluster, namespace, and the two vendored Helm charts
kind create cluster --config k8s/kind-config.yaml --name bpa
kubectl create namespace airflow
helm install spark-operator ./k8s/helm/spark-operator --namespace airflow

# 2. cassandra, and its schema (generated from infra/cassandra/schema.cql,
#    the single source of truth also used by Docker Compose)
kubectl apply -f k8s/manifests/cassandra.yml
kubectl apply -f k8s/manifests/volumes.yml
kubectl create configmap cassandra-schema --from-file=schema.cql=infra/cassandra/schema.cql --namespace airflow
kubectl apply -f k8s/manifests/cassandra-schema-init-job.yml

# 3. build the 4 job images and the custom airflow image, load them into the cluster
python3 scripts/fetch_data.py   # if you haven't already
for job in ingest_raw_data validate_raw_data compute_metrics load_metrics; do
  docker build -t ${job}:latest ./jobs/${job}
  kind load docker-image ${job}:latest --name bpa
done
docker build -t airflow:1.0 -f k8s/Dockerfile .
kind load docker-image airflow:1.0 --name bpa

# 4. airflow itself, plus the one-time setup it needs
helm install airflow ./k8s/helm/airflow --namespace airflow \
  -f k8s/airflow-local-values.yaml \
  --set postgresql.image.repository=bitnamilegacy/postgresql
kubectl apply -f k8s/manifests/airflow-spark-rbac.yml
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow connections add kubernetes_default --conn-type kubernetes --conn-extra '{"in_cluster": true}'

# 5. trigger it
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow dags unpause batch_pipeline_automation
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow dags trigger batch_pipeline_automation
```

Watch it run:

```bash
kubectl -n airflow get sparkapplications -w
```

A few things worth knowing if you're adapting this:

- **`k8s/airflow-local-values.yaml`** trims the vendored Airflow chart down for a single-DAG local cluster (KubernetesExecutor instead of Celery, no triggerer, no metrics exporter) and fixes two real bugs hit standing this up: a Bitnami postgres image that generates a random password independent of the one Airflow actually connects with, and a [confirmed upstream memory leak](https://github.com/apache/airflow/issues/29841) in Airflow 2.5.1's webserver (fixed by pinning 2.5.2 in `k8s/Dockerfile`).
- **The `kubernetes_default` connection and the RBAC grant** aren't optional. Airflow's `SparkKubernetesOperator` needs a `kubernetes_default` connection to exist (it isn't seeded by default), and neither the Airflow chart nor the Spark Operator chart grants Airflow's own service accounts permission to manage `SparkApplication` resources.
- **`k8s/kind-config.yaml`** mounts `./data/raw` into the cluster so `ingest_raw_data` can read the CSVs `fetch_data.py` downloaded on the host. `compute_metrics`'s output uses KinD's own dynamic storage class instead, since nothing needs to pre-seed it from the host.

## Tests

```bash
sbt test
```

Real ScalaTest coverage for the pure logic in each job (data quality checks, the promo/baseline/forecast math, CSV-to-Cassandra casts), run in CI on every push.

## Layout

```
jobs/
  common/              shared Spark session + Cassandra helpers
  ingest_raw_data/     raw CSVs -> Cassandra
  validate_raw_data/   data quality checks
  compute_metrics/     the actual metrics
  load_metrics/        parquet -> Cassandra
infra/cassandra/       schema.cql, the single source of truth for the schema
airflow/dags/          the Airflow DAG
k8s/                   Helm charts, manifests, and the local KinD setup
scripts/fetch_data.py  downloads and reshapes the real dataset
```
