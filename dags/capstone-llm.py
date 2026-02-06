from datetime import datetime, timedelta
from airflow import DAG
#from airflow.operators.bash import BashOperator
#from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from conveyor.operators import ConveyorContainerOperatorV2

#role = "capstone_conveyor_llm"

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

    #t1 = BashOperator(task_id="print_current_date", bash_command="date")

    # clean_data = DockerOperator(
    #     task_id="clean_data",
    #     image="myimage:latest",
    #     container_name="clean_task",
    #     api_version="auto",
    #     auto_remove="force",
    #     command="python3 -m capstonellm.tasks.clean",
    #     docker_url="unix://var/run/docker.sock",
    #     network_mode="bridge",
    #     environment={
    #         "AWS_ACCESS_KEY_ID": "{{ var.value.aws_access_key_id_an }}",
    #         "AWS_SECRET_ACCESS_KEY": "{{ var.value.aws_secret_access_key_an }}",
    #     }
    # )

    clean_data = ConveyorContainerOperatorV2(
        dag=dag,
        task_id="clean_data",
        command=["python3", "-m", "capstonellm.tasks.clean"],
        aws_role="capstone_conveyor_llm",
        instance_type='mx.micro',
    )

    start_dag >> clean_data >> end_dag