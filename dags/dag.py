# Sparkify - Data Engineer Nanodegree program Sparkify Data Pipeline
# By JGEL
# May 2020

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

# DAG default configuration
default_args = {
    'owner': 'jerryespn',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# DAG Declaration 
dag = DAG('dag-project-dtpipeline',
          default_args=default_args,
          description='Load and transform data from AWS S3 in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Tables creation

tables_creation = PostgresOperator(
    task_id = 'table_creation',
    dag = dag,
    postgres_con_id = 'redshift',
    sql = '/create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'log-data/{execution_date.year}/{execution_date.month:02d}',
    table = 'staging_events',
    file_format = 'JSON \'s3://udacity-dend/log_json_path.json\''
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    table = 'staging_songs',
    conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/A/A',
    file_format = 'JSON \'auto\''
)

# Starts data loading

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = 'songplays',
    conn_id = 'redshift',
    sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    table = 'users',
    fields = 'userid, first_name, last_name, gender, level',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.user_table_insert,
    append_data = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    table = 'songs',
    fields = 'songid, title, artistid, year, duration ',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.song_table_insert,
    append_data = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    table = 'artists',
    fields = 'artistid, name, location, lattitude, longitude',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.artist_table_insert,
    append_data = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    table = 'times',
    fields = 'start_time, hour, day, week, month, year, week_day',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.time_table_insert,
    append_data = False
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    tables = ['songplays', 'songs', 'artists', 'users', 'times'],
    redshift_conn_id = 'redshift',
)

end_operator = DummyOperator(task_id = 'Stop_execution',  dag=dag)

start_operator >> tables_creation
tables_creation >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
