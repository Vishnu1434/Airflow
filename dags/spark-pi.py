from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

NAMESPACE = "spark-jobs"
SPARK_IMAGE = "ghcr.io/vishnu1434/spark-scala:main"
SPARK_MASTER_URL = "k8s://https://kubernetes.default.svc"
APP_CLASS = "demo.App"   # <-- change if your main class has a different name
APP_JAR = "local:///opt/spark-app/app.jar"
EXECUTOR_INSTANCES = 2

with DAG(
    dag_id="test-run-1",
    start_date=datetime(2025, 8, 15, 9),
    schedule_interval="@hourly",
    catchup=True,
    tags=["spark", "k8s"],
    max_active_runs=1,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    start = EmptyOperator(task_id="start")

    spark_submit = KubernetesPodOperator(
        task_id="spark_submit_job",
        name="submitter",  # submitter pod name
        namespace="airflow",  # Airflow namespace
        image=SPARK_IMAGE,
        cmds=["/opt/bitnami/spark/bin/spark-submit"],
        arguments=[
            "--master", SPARK_MASTER_URL,
            "--deploy-mode", "cluster",
            "--class", APP_CLASS,
            "--conf", "spark.jars.ivy=/tmp/.ivy2",  # absolute ivy cache
            "--conf", f"spark.executor.instances={EXECUTOR_INSTANCES}",
            "--conf", "spark.kubernetes.namespace=spark-space",  # driver/executors in spark-space
            "--conf", "spark.kubernetes.driver.pod.name=spark-driver",
            "--conf", "spark.kubernetes.executor.podNamePrefix=spark-exec",
            APP_JAR
        ],
        env_vars={
            "HOME": "/opt/airflow",
            "SPARK_HOME": "/opt/bitnami/spark",
        },
        get_logs=True,
        is_delete_operator_pod=False,  # keep submitter pod for debugging
        in_cluster=True,
    )

    end = EmptyOperator(task_id="end")

    start >> spark_submit >> end
