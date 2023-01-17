
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

AWS_KEY = ''
AWS_SECRET = ''
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5) 
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
         )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
               
stage_events_to_redshift = StageToRedshiftOperator(
    task_id ='Stage_events',
    dag = dag,
    table = 'staging_events',
    redshift_conn_id = 'redshift',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data/{execution.year}/{execution.month}',
    s3_json = 'log_data/log_json_path.json',
    aws_key = AWS_KEY,
    aws_secret = AWS_SECRET,
    provide_context = True)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag = dag,
    table = 'staging_songs',
    redshift_conn_id = 'redshift',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/',
    s3_json = 'auto',
    aws_key = AWS_KEY,
    aws_secret = AWS_SECRET,
    provide_context = True)


load_songplays_table = LoadFactOperator(
    dag=dag,
    table = 'songplays',
    redshift_conn_id = 'redshift',
    sql=SqlQueries.songplay_table_insert,
    action = 'append',
    task_id='Load_songplays_fact_table')


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = 'users',
    redshift_conn_id = 'redshift',
    sql=SqlQueries.user_table_insert,
    action = 'truncate')

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    table = 'songs',
    redshift_conn_id = 'redshift',
    sql = SqlQueries.song_table_insert,
    action = 'truncate')

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    table = 'artists',
    redshift_conn_id = 'redshift',
    sql = SqlQueries.artist_table_insert,
    action = 'truncate')

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    table = 'time',
    redshift_conn_id = 'redshift',
    sql = SqlQueries.time_table_insert,
    action = 'truncate')

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    tables = {'table_field':['users', 'userid'], 
                  'table_field':['songs', 'songid'],
                  'table_field':['artists', 'artistid'],
                  'table_field':['time', 'start_time'],
                  'table_fied':['songplays', 'playid']})

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >>[stage_events_to_redshift, stage_songs_to_redshift]\
               >> load_songplays_table\
               >> [load_user_dimension_table, load_song_dimension_table, \
                   load_artist_dimension_table, load_time_dimension_table]\
               >> run_quality_checks\
               >> end_operator