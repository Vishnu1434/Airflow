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

    spark_submit = KubernetesPodOperator(
        task_id="spark_submit_job",
        name="spark-submitter",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=['/opt/bitnami/spark/bin/spark-submit'],
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
            "--conf", f"spark.kubernetes.kerberos.krb5.conf={KRB5_CONF_PATH}",
            "--conf", f"spark.kubernetes.kerberos.keytab={KEYTAB_PATH_IN_PVC}",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
            f"local://{APP_JAR_PATH_IN_PVC}"
        ],
        volumes=[spark_jar_volume, spark_keytab_volume, krb5_conf_volume],
        volume_mounts=[spark_jar_mount, spark_keytab_mount, krb5_conf_mount],
        get_logs=True,
        is_delete_operator_pod=False,
        in_cluster=True,
        env_vars={
            "USER": "airflow",
            "HOME": "/opt/airflow",
            "SPARK_HOME": "/opt/bitnami/spark",
            "KRB5_CONFIG": KRB5_CONF_PATH,
            "KRB5_CLIENT_KTNAME": KEYTAB_PATH_IN_PVC
        }
    )

    end = EmptyOperator(task_id="end")

    start >> spark_submit >> end
