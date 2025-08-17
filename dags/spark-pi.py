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
KEYTAB_PATH_IN_PVC = "/mnt/spark-keytabs/krb5.keytab"
KRB5_CONF_PATH = "/etc/krb5/krb5.conf"
EXECUTOR_INSTANCES = 2

# PVC for Spark JARs
spark_jar_volume = k8s.V1Volume(
    name="spark-jar-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="spark-jar-pvc")
)
spark_jar_mount = k8s.V1VolumeMount(
    name="spark-jar-volume",
    mount_path="/mnt/spark-jars",
    read_only=True
)

# PVC or Secret for keytab
spark_keytab_volume = k8s.V1Volume(
    name="spark-keytab-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="spark-keytab-pvc")
)
spark_keytab_mount = k8s.V1VolumeMount(
    name="spark-keytab-volume",
    mount_path="/mnt/spark-keytabs",
    read_only=True
)

# ConfigMap for krb5.conf
krb5_conf_volume = k8s.V1Volume(
    name="krb5-conf-volume",
    config_map=k8s.V1ConfigMapVolumeSource(name="krb5-conf")
)
krb5_conf_mount = k8s.V1VolumeMount(
    name="krb5-conf-volume",
    mount_path="/etc/krb5",
    read_only=True
)

spark_cmd = """
/opt/bitnami/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --name demo-app \
  --class demo.App \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.executor.instances=2 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.container.image=bitnami/spark:3.2.4 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.jars.mount.path=/mnt/jars \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.jars.claimName=spark-jar-pvc \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.jars.mount.path=/mnt/jars \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.jars.claimName=spark-jar-pvc \
  local:///mnt/jars/app.jar
"""


with DAG(
    dag_id="spark_job_with_kerberos",
    start_date=datetime(2025, 8, 16),
    schedule_interval="@once",
    catchup=False,
    tags=["spark", "k8s", "kerberos"],
    max_active_runs=1,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    start = EmptyOperator(task_id="start")

    spark_operator = KubernetesPodOperator(
        task_id="spark_task",
        name="spark-launch",
        namespace="spark-jobs",
        image="bitnami/spark:3.2.4",
        cmds=["/bin/bash", "-c"],
        arguments=[spark_cmd],
        is_delete_operator_pod=True,
        env_vars={"HADOOP_CONF_DIR": "", "KRB5_CONFIG": "", "HOME": "/tmp"}
    )

    end = EmptyOperator(task_id="end")

    start >> spark_operator >> end
