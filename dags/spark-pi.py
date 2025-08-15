from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id="spark_job_in_other_namespace",
    start_date=datetime(2025, 8, 15, 0),
    schedule_interval="@hourly",
    catchup=True,
    tags=["spark", "k8s"],
) as dag:

    start = DummyOperator(task_id="start")

    spark_job = KubernetesPodOperator(
        task_id="spark_pi_job",
        name="spark-pi",
        namespace="spark-jobs",  # Target namespace
        image="bitnami/spark:3.3.1",  # Spark image
        cmds=["/opt/bitnami/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc",  # Spark on K8s
            "--deploy-mode", "cluster",
            "--class", "org.apache.spark.examples.SparkPi",
            "local:///opt/bitnami/spark/examples/jars/spark-examples_2.12-3.3.1.jar",
            "10"
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    end = DummyOperator(task_id="end")

    start >> spark_job >> end
