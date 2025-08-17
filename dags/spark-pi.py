from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="spark_operator_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": "demo-app",
            "namespace": "spark-jobs"
        },
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "sparkVersion": "3.2.4",
            "image": "bitnami/spark:3.2.4",
            "mainClass": "demo.App",
            "mainApplicationFile": "local:///mnt/jars/app.jar",
            "driver": {
                "cores": 1,
                "memory": "512m",
                "serviceAccount": "spark"
            },
            "executor": {
                "cores": 1,
                "instances": 2,
                "memory": "512m"
            }
        }
    }

    spark_submit = SparkKubernetesOperator(
        task_id="spark_submit",
        namespace="spark-jobs",
        application_manifest=spark_app,  # 👈 correct field
        do_xcom_push=True
    )
