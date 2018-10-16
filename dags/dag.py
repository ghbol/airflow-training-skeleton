import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)

from airflow.utils.trigger_rule import TriggerRule

from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

# project_id = "airflowbolcom-4a6a6874aa7bb00c"
project_id = "training-airflow"

dag = DAG(
    dag_id="uk_land_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 10, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)

pg_2_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="pg_2_gcs",
    postgres_conn_id="my_db_connection",
    sql="SELECT COUNT(*) FROM land_registry_price_paid_uk",
    bucket="airflowbolcom_ghermann_dummybucket",
    filename="mypgdata",
    dag=dag
)

zone = "europe-west4-a"

dataproc_cluster_name = "my_dp_cluster_{{ ds }}"

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="my_create_dp_cluster",
    cluster_name=dataproc_cluster_name,
    project_id=project_id,
    num_workers=2,
    zone=zone,
    dag=dag,
)

compute_aggregates = DataProcPySparkOperator(
    task_id="my_compute_aggregates",
    main='gs://gdd-training-bucket/build_statistics.py',
    cluster_name=dataproc_cluster_name,
    arguments=["{{ ds }}"],
    project_id=project_id,
    dag=dag,
)


dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="my_delete_dp_cluster",
    cluster_name=dataproc_cluster_name,
    project_id=project_id,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

pg_2_gcs >> dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster


def print_exec_date(**context):
    print(context["execution_date"])


my_task = PythonOperator(
    task_id="task_name", python_callable=print_exec_date, provide_context=True, dag=dag
)
