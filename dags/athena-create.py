from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
import os
import boto3
default_args= {
    'owner':'phidelist',
    'depends_on_past':False,
    'email':'delph@gmail.com',
    'email_on_failure':False,
    'email_on_retry':False,
}

DAG_ID = os.path.basename(__file__).replace('.py', '')

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Athena to create SciFi DAG',
    schedule_interval=None,
    start_date=datetime(2021,10,26),
    tags=['airflow','automation']
)

# Set Variables used in tasks and stored in AWS Secrets Manager

s3_dlake = Variable.get("s3_dlake", default_var="undefined")
s3_data = Variable.get("s3_data", default_var="undefined")
athena_db = Variable.get("athena_db", default_var="undefined")
athena_output = Variable.get("athena_output", default_var="undefined")

def py_display_variables(**kwargs):
    print("Data Lake location " + s3_dlake + " ")
    print("Data within Lake " + s3_data + " ")
    print("New Athena DB " + athena_db + " ")
    print("Output CSV file we create " + athena_output + " ")

disp_variables = PythonOperator (
	task_id='print_variables',
	provide_context=True,
	python_callable=py_display_variables,
	dag=dag
	)

def check_athena_database(**kwargs):
    ath = boto3.client('athena')
    try:
        response = ath.get_database(
            CatalogName='AwsDataCatalog',
            DatabaseName=athena_db
        )
        print("Database already exists - skip creation")
        return "skip_athena_database_creation"
        #return "check_athena_export_table_done"
    except:
        print("No Database Found")
        return "create_athena_database"
    

check_athena_database = BranchPythonOperator(
    task_id='check_athena_database',
    provide_context=True,
    python_callable=check_athena_database,
    retries=1,
    dag=dag,
)