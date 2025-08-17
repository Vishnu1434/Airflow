from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="spark_job_operator",
    start_date=datetime(2025, 8, 16),
    schedule_interval="@once",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    spark_submit = BashOperator(
        task_id="spark_submit_task",
        bash_command="""
            /opt/bitnami/spark/bin/spark-submit \
              --master k8s://https://kubernetes.default.svc \
              --deploy-mode cluster \
              --name demo-app \
              --class demo.App \
              --conf spark.executor.instances=2 \
              --conf spark.kubernetes.namespace=spark-jobs \
              --conf spark.kubernetes.container.image=bitnami/spark:3.2.4 \
              --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
              --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.jars.options.claimName=spark-jar-pvc \
              --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.jars.mount.path=/mnt/jars \
              --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.jars.options.claimName=spark-jar-pvc \
              --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.jars.mount.path=/mnt/jars \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              local:///mnt/jars/app.jar
            """
    )

    end = EmptyOperator(task_id="end")

    start >> spark_submit >> end
