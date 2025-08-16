from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from kubernetes.client import models as k8s

NAMESPACE = "spark-jobs"
SPARK_IMAGE = "bitnami/spark:3.2.4"
SPARK_MASTER_URL = "k8s://https://kubernetes.default.svc"
APP_CLASS = "demo.App"    # change if your main class is different
APP_JAR_PATH_IN_PVC = "/mnt/spark-jars/app.jar"
EXECUTOR_INSTANCES = 2

# Define Volume and VolumeMount for the PVC
spark_jar_volume = k8s.V1Volume(
    name="spark-jar-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="spark-jar-pvc")
)
spark_jar_mount = k8s.V1VolumeMount(
    name="spark-jar-volume",
    mount_path="/mnt/spark-jars",
    read_only=True
)

with DAG(
    dag_id="spark_job_with_pvc_jar",
    start_date=datetime(2025, 8, 16),
    schedule_interval="@once",
    catchup=False,
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
        name="spark-submitter",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=['/opt/bitnami/spark/bin/spark-submit --conf spark.driver.extraJavaOptions="-Dhadoop.security.authentication=simple"'],
        arguments=[
            "--master", SPARK_MASTER_URL,
            "--deploy-mode", "cluster",
            "--class", APP_CLASS,
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--conf", f"spark.executor.instances={EXECUTOR_INSTANCES}",
            "--conf", f"spark.kubernetes.namespace={NAMESPACE}",
            "--conf", "spark.kubernetes.driver.pod.name=spark-driver",
            "--conf", "spark.kubernetes.executor.podNamePrefix=spark-exec",
            "--conf", f"spark.kubernetes.driver.container.image={SPARK_IMAGE}",
            "--conf", "spark.hadoop.hadoop.security.authentication=simple",
            "--conf", "spark.hadoop.hadoop.security.authorization=false",
            f"local://{APP_JAR_PATH_IN_PVC}"
        ],
        volumes=[spark_jar_volume],
        volume_mounts=[spark_jar_mount],
        get_logs=True,
        is_delete_operator_pod=False,   # keep submitter pod for debugging
        in_cluster=True,
        env_vars={
            "USER": "airflow",
            "HOME": "/opt/airflow",
            "SPARK_HOME": "/opt/bitnami/spark"
        }
    )

    end = EmptyOperator(task_id="end")

    start >> spark_submit >> end
