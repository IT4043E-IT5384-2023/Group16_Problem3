# Group16_Problem5
## High risk transaction detection
------------------

## Set up
1. Clone the repository
2. cd to the repo directory
3. Run `pip install -r requirements.txt` in your terminal or command prompt
4. Set up airflow apache to schedule task
* Follow this guide: https://medium.com/@ericfflynn/installing-apache-airflow-with-pip-593717580f86
## Usage
1. To run the high_risk_transaction_detection function, use the following code snippet within an Airflow DAG:
- The file "big_data_dag.py" should be created and saved in folder "./dags" which is the same as the current folder of Airflow working folder.
The content of "big_data_dag.py" is presented as below:
```bash
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


from proj_repo.pipeline import data_scrape, processing



with DAG(dag_id="big_data_dag",
         start_date=datetime(2023, 12, 23),
         schedule_interval= timedelta(minutes=30),
         catchup=False) as dag:
    task1 = PythonOperator(
        task_id="load_data",
        python_callable=data_scrape.main
    )
    
    task2 = PythonOperator(
        task_id="train_model", 
        python_callable=processing.process,
        trigger_rule='all_success', # Set this parameter to all_done
    )

task1 >> task2 # This means task_2 will run after task_1

```


2. To view the dashboard, please visit: [High Risk Transaction Detection Dashboard](http://34.143.255.36:5601/s/it4043e---group16/app/dashboards#/view/f6f8a710-a13a-11ee-8d94-5d4fdf5aea4c?_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:now-30d%2Fd,to:now)))

3. Please find our tele-bot by id *@crypto_centic_bot* to get briefly report.
