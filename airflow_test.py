from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
from kubernetes.client import models as k8s


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}


dag = DAG(
    "preprocess_and_train",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)

start = DummyOperator(
    task_id="start",
    dag=dag,
)

preprocess = KubernetesPodOperator(
    namespace="airflow",
    image="hongtaekim/k8s-airflow-test:latest",
    cmds=["python3", "preprocess.py"],
    name="preprocess",
    task_id="preprocess_task",
    volumes=[
        k8s.V1Volume(
            name="airflow-pv",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name="airflow-pvc"
            ),
        )
    ],
    volume_mounts=[
        # k8s.V1VolumeMount(mount_path="/mnt/data/airflow", name="airflow-pv")
        k8s.V1VolumeMount(mount_path="/app/data", name="airflow-pv")
    ],
    get_logs=True,
    dag=dag,
)

train = KubernetesPodOperator(
    namespace="airflow",
    image="hongtaekim/k8s-airflow-test:latest",
    cmds=["python3", "train.py"],
    name="train",
    task_id="train_task",
    volumes=[
        k8s.V1Volume(
            name="airflow-pv",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name="airflow-pvc"
            ),
        )
    ],
    volume_mounts=[
        # k8s.V1VolumeMount(mount_path="/mnt/data/airflow", name="airflow-pv")
        k8s.V1VolumeMount(mount_path="/app/data", name="airflow-pv")
    ],
    get_logs=True,
    dag=dag,
)

end = DummyOperator(
    task_id="end",
    dag=dag,
)

start >> preprocess >> train >> end
