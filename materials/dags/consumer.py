from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset("/tmp/my_file.txt")
my_file2 = Dataset("/tmp/my_file2.txt")


with DAG(
    dag_id="consumer",
    schedule=[my_file, my_file2],
    start_date=datetime(2022,1,1),
    catchup=False
):
    
    @task()
    def  read_dataset():
        with open(my_file.uri, "a+") as f:
            print(f.read())

    read_dataset()
