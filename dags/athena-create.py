from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.hooks.base_hook import BaseHook
import os
import boto3
default_args= {
    'owner':'phidelist',
    'depends_on_past':False,
    'email':'delph@gmail.com',
    'email_on_failure':False,
    'email_on_retry':False,
}

aws_conn_id = 'aws-default'
aws_connection = BaseHook.get_connection(aws_conn_id)

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

s3_dlake = Variable.get("s3_dlake", default_var="globalphidelist.tech")
region= Variable.get("region", default_var="us-east-1")
workgroup= Variable.get("workgroup", default_var="primary")
s3_data = Variable.get("s3_data", default_var="undefined")
athena_db = Variable.get("athena_db", default_var="athena_db")
athena_output = Variable.get("athena_output", default_var="undefined")

# Define the SQL we want to run to create the tables for our new venture

create_athena_movie_table_query="""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.movies (
  `movieId` int,
  `title` string,
  `genres` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://{s3_dlake}/movielens/movies/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'skip.header.line.count'='1'
); 
""".format(database=athena_db, s3_dlake=s3_dlake)

create_athena_ratings_table_query="""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.ratings (
  `userId` int,
  `movieId` int,
  `rating` int,
  `timestamp` bigint 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://{s3_dlake}/movielens/ratings/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'skip.header.line.count'='1'
); 
""".format(database=athena_db, s3_dlake=s3_dlake)

create_athena_scifi_table_query = """
CREATE TABLE IF NOT EXISTS {database}.scifi AS WITH scifidata AS (
SELECT REPLACE ( m.title , '"' , '' ) as title, r.rating
FROM {database}.movies m
INNER JOIN (SELECT rating, movieId FROM {database}.ratings) r on m.movieId = r.movieId WHERE REGEXP_LIKE (genres, 'Sci-Fi')
)
SELECT title, replace(substr(trim(title),-5),')','') as year, AVG(rating) as avrating from scifidata GROUP BY title ORDER BY year DESC,  title ASC;
""".format(database=athena_db)

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
    ath = boto3.client('athena',region_name='us-east-1',
                       aws_access_key_id=aws_connection.login,
                       aws_secret_access_key=aws_connection.password)
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
    
def create_db(**kwargs):
    print("creating the database if it does not exist")
    ath = boto3.client('athena', region_name='us-east-1',
                       aws_access_key_id=aws_connection.login,
                       aws_secret_access_key=aws_connection.password)
    ath.start_query_execution(
        QueryString=f'CREATE DATABASE IF NOT EXISTS {athena_db}',
        ResultConfiguration={'OutputLocation': 's3://{s3_dlake}/queries/'.format(s3_dlake=s3_dlake)},
        WorkGroup="primary"
    )


check_athena_database = BranchPythonOperator(
    task_id='check_athena_database',
    provide_context=True,
    python_callable=check_athena_database,
    retries=1,
    dag=dag,
)
skip_athena_database_creation = DummyOperator(
    task_id="skip_athena_database_creation",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

create_athena_database = PythonOperator (
    task_id='create_athena_database',
	provide_context=True,
	python_callable=create_db,
	dag=dag
    )

athena_database_checks_done = DummyOperator(
    task_id="athena_database_checks_done",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)


create_athena_movie_table = AWSAthenaOperator(
    task_id="create_athena_movie_table",
    query=create_athena_movie_table_query, 
    workgroup = workgroup, 
    database=athena_db,
    output_location='s3://'+s3_dlake+"/"+athena_output+'create_athena_movie_table',
    region_name=region,
    aws_conn_id='aws-default'
    )
create_athena_ratings_table = AWSAthenaOperator(
    task_id="create_athena_movie_ratings",
    query=create_athena_ratings_table_query, 
    workgroup = workgroup, 
    database=athena_db,
    output_location='s3://'+s3_dlake+"/"+athena_output+'create_athena_ratings_table',
    region_name=region,
    aws_conn_id='aws-default'
    )
create_athena_scifi_table = AWSAthenaOperator(
    task_id="create_athena_scifi_table",
    query=create_athena_scifi_table_query, 
    workgroup = workgroup, 
    database=athena_db,
    output_location='s3://'+s3_dlake+"/"+athena_output+'create_athena_scifi_table',
    region_name=region,
    aws_conn_id='aws-default'
    )

disp_variables >> check_athena_database

check_athena_database >> skip_athena_database_creation >> athena_database_checks_done >> create_athena_movie_table >> create_athena_ratings_table >> create_athena_scifi_table

check_athena_database >> create_athena_database >> athena_database_checks_done >> create_athena_movie_table >> create_athena_ratings_table >> create_athena_scifi_table