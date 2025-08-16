from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from kubernetes.client import V1Volume, V1VolumeMount, V1PersistentVolumeClaimVolumeSource

NAMESPACE = "spark-jobs"
SPARK_IMAGE = "ghcr.io/vishnu1434/spark-scala:main"
SPARK_MASTER_URL = "k8s://https://kubernetes.default.svc"
APP_CLASS = "demo.App"
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
        name="submitter",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=["/opt/bitnami/spark/bin/spark-submit"],
        arguments=[
            "--master", SPARK_MASTER_URL,
            "--deploy-mode", "cluster",
            "--class", APP_CLASS,
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--conf", f"spark.executor.instances={EXECUTOR_INSTANCES}",
            "--conf", f"spark.kubernetes.namespace={NAMESPACE}",
            "--conf", "spark.kubernetes.driver.pod.name=spark-driver",
            "--conf", "spark.kubernetes.executor.podNamePrefix=spark-exec",
            APP_JAR,
        ],
        env_vars={
            "HOME": "/opt/airflow",
            "SPARK_HOME": "/opt/bitnami/spark",
        },
        volumes=[V1Volume(
            name="spark-jar",
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name="spark-jar-pvc")
        )],
        volume_mounts=[V1VolumeMount(
            name="spark-jar",
            mount_path="/opt/spark-app"
        )],
        get_logs=True,
        is_delete_operator_pod=False,
        in_cluster=True,
    )

    end = EmptyOperator(task_id="end")

    start >> spark_submit >> end
