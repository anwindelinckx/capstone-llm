from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from conveyor.operators import ConveyorContainerOperatorV2


default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "clean_data_dag",
    default_args=default_args,
    schedule="5 * * * *",
    catchup=False,
) as dag:
    start_dag = EmptyOperator(task_id="start_dag")

    end_dag = EmptyOperator(task_id="end_dag")

    clean_data = ConveyorContainerOperatorV2(
        dag=dag,
        task_id="clean_data",
        command=["python3", "-m", "capstonellm.tasks.clean"],
        aws_role="capstone_conveyor_llm",
        instance_type='mx.micro',
        arguments=["--env", "winterschool"]
        )

    start_dag >> clean_data >> end_dag