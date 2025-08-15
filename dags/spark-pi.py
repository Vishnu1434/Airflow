from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}


with DAG(
    dag_id='spark_pi_k8s',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run Spark Pi job via KubernetesPodOperator',
) as dag:

    spark_pi = KubernetesPodOperator(
        task_id='spark_pi_task',
        namespace='spark-operator',  # Run in spark-operator namespace
        name='spark-pi',
        image='bitnami/spark:3.1.1',
        cmds=['/opt/bitnami/spark/bin/spark-submit'],
        arguments=[
            '--master', 'k8s://https://kubernetes.default.svc',
            '--deploy-mode', 'cluster',
            '--class', 'org.apache.spark.examples.SparkPi',
            'local:///opt/bitnami/spark/examples/jars/spark-examples_2.12-3.1.1.jar',
            '1000'
        ],
        get_logs=True,
        is_delete_operator_pod=True
    )
