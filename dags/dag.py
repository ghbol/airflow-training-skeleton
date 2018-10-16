import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

dag = DAG(
    dag_id="uk_land_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 8, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)

pg_2_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="pg_2_gcs",
    postgres_conn_id="my_db_connection",
    sql="SELECT COUNT(*) FROM gdd.land_registry_price_paid_uk",
    bucket="airflowbolcom_ghermann_dummybucket",
    filename="mypgdata",
    dag=dag
)


def print_exec_date(**context):
    print(context["execution_date"])


my_task = PythonOperator(
    task_id="task_name", python_callable=print_exec_date, provide_context=True, dag=dag
)
