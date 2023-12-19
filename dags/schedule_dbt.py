from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta

dir = "working dir"
env_path = "/path/to/env" # path to virtual env that has dbt installed
dbt_project_dir = "/path/to/dbt_project" # dbt project dir
dbt_profile_dir = "/path/to/.dbt" # where the file profiles.yml is stored

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['example@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id ='dbt_transformations',
    default_args= default_args,
    description = "Dag to run dbt transformation",
    start_date = datetime(2023,12,20,2),
    schedule_interval = None,
    # schedule_interval = timedelta(minutes=10),
    catchup = False,
) as dag:
    
    run_dbt = BashOperator(
        task_id = "run_dbt",
        bash_command = f'''
        cd {dir} &&\
             source {env_path}/bin/activate &&\
                 dbt run --project-dir {dbt_project_dir} --profiles-dir {dbt_profile_dir}
                 ''',
    )
    

    test_dbt = BashOperator(
        task_id= "test_dbt",
        bash_command = f'''
        cd {dir} &&\
             source {env_path}/bin/activate &&\
                 dbt test --project-dir {dbt_project_dir} --profiles-dir {dbt_profile_dir} --store-failures
                 ''',
    )

    run_dbt >> test_dbt
