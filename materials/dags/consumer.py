from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset("/tmp/my_file.txt")


with DAG(
    dag_id="consumer",
    schedule=[my_file],
    start_date=datetime(2022,1,1),
    catchup=False
):
    
    @task
    def  read_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("producer update")

    read_dataset()
