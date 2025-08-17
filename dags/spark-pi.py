from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="spark_operator_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_spark_app = SparkKubernetesOperator(
        task_id="submit_spark_app",
        namespace="spark-jobs",  # ✅ where SparkApplication CR should live
        application_file="/opt/airflow/dags/spark-app.yaml",  # ✅ YAML inside Airflow's DAG folder
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )
