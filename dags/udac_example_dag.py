from datetime import datetime, timedelta
import pendulum
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import dag, task
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=300),
    'catchup': False,
    'email_on_retry': False,
}

@dag(
      default_args=default_args,
      description='Load and transform data in Redshift with Airflow',
      schedule_interval='0 * * * *'
    )
def udac_example_dag(**kwargs):

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="katya-bucket-redshift2",
        s3_key=f"log-data/{kwargs['execution_date'].year}/{kwargs['execution_date'].month}/{kwargs['ds']}-events.json",
        region="us-west-2"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="katya-bucket-redshift2",
        s3_key="song-data",
        region="us-west-2"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.songplay_table_insert,
        insert_table="songplays",
    )


    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        insert_table="users",
        delete=True,
        sql_query=SqlQueries.user_table_insert,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        insert_table="songs",
        delete=True,
        sql_query=SqlQueries.song_table_insert,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        insert_table="artists",
        delete=True,
        sql_query=SqlQueries.artist_table_insert,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        insert_table="time",
        delete=True,
        sql_query=SqlQueries.time_table_insert,
    )


    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        table="staging_events",
        column="userid"
    )

    end_operator = DummyOperator(task_id='Stop_execution')


    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table


    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                             load_time_dimension_table] >> run_quality_checks >> end_operator


udac_example = udac_example_dag()