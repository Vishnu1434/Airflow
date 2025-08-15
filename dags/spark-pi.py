from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# Spark job configuration
SPARK_IMAGE = "bitnami/spark:3.3.1"
NAMESPACE = "spark-space"
SPARK_MASTER_URL = "k8s://https://kubernetes.default.svc"
APP_CLASS = "org.apache.spark.examples.SparkPi"
APP_JAR = "local:///opt/bitnami/spark/examples/jars/spark-examples_2.12-3.3.1.jar"
EXECUTOR_INSTANCES = "2"

with DAG(
    dag_id="spark_cluster_job_with_executors",
    start_date=datetime(2025, 8, 15, 9),
    schedule_interval=None,
    catchup=True,
    tags=["spark", "k8s"],
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    start = DummyOperator(task_id="start")

    spark_submit = KubernetesPodOperator(
        task_id="spark_submit_job",
        name="spark-pi-job",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=["/opt/bitnami/spark/bin/spark-submit"],
        arguments=[
            "--master", SPARK_MASTER_URL,
            "--deploy-mode", "cluster",         # driver runs as pod
            "--class", APP_CLASS,
            "--conf", f"spark.executor.instances={EXECUTOR_INSTANCES}",  # 2 executors
            APP_JAR,
            "10"
        ],
        get_logs=True,
        is_delete_operator_pod=False,  # Keep driver pod for debugging if fails
        in_cluster=True,
        # Optional: you can add nodeSelector, tolerations, or resources here
    )

    end = DummyOperator(task_id="end")

    start >> spark_submit >> end
