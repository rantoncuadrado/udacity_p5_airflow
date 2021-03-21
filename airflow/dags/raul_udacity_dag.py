from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
s3_bucket = 'udacity-dend'
song_s3_key = "song_data"
log_s3_key = "log_data"
log_file = "log_json_path.json"

default_args = {
    'owner': 'Raul',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 2),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
#   parameters for testing purposes: max_active_runs and max_active_runs_per_dag
#   'max_active_runs_per_dag' : 3,
#   'max_active_runs': 3,
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG('Raul_project_DAG',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
#         schedule_interval='@once' is useful for testing
#         schedule_interval='@once',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = 'staging_events',
    s3_bucket = s3_bucket,
    s3_key = log_s3_key,
    region='us-west-2',
    file_format = 'JSON',
    log_file = log_file,
    redshift_conn_id = 'redshift',
    aws_conn_id = 'aws_credentials',
    provide_context = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    table = 'staging_songs',
    s3_bucket = s3_bucket,
    s3_key = song_s3_key,
    region='us-west-2',
    file_format = 'JSON',
    redshift_conn_id = 'redshift',
    aws_conn_id = 'aws_credentials',
    provide_context = True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="songplays",
    truncate_table=True,
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='users',
    truncate_table=True,
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songs',
    truncate_table=True,
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='artists',
    truncate_table=True,
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='time',
    truncate_table=True,
    sql_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table_names=['songplays', 'users', 'songs', 'artists', 'time'],
    table_pks=['userid', 'userid', 'songid', 'artistid', 'start_time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

## Dependencies

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator