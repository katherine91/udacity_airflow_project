import pendulum
import psycopg2

from airflow.decorators import dag, task
from airflow.operators.postgres_operator import PostgresOperator

from create_table import create_query

@dag(
    start_date=pendulum.now()
)
def create_tables_dag():

    create_staging_songs_table = PostgresOperator(
        task_id="create_staging_songs_table",
        postgres_conn_id="redshift",
        sql=create_query.staging_songs_table
    )

    create_staging_events_table = PostgresOperator(
        task_id="create_staging_events_table",
        postgres_conn_id="redshift",
        sql=create_query.staging_events_table
    )

    create_time_table = PostgresOperator(
        task_id="create_time_table",
        postgres_conn_id="redshift",
        sql=create_query.time_table
    )

    create_songs_table = PostgresOperator(
        task_id="create_songs_table",
        postgres_conn_id="redshift",
        sql=create_query.songs_table
    )

    create_users_table = PostgresOperator(
        task_id="create_users_table",
        postgres_conn_id="redshift",
        sql=create_query.users_table
    )

    create_artists_table = PostgresOperator(
        task_id="create_artists_table",
        postgres_conn_id="redshift",
        sql=create_query.artists_table
    )

    create_songplays_table = PostgresOperator(
        task_id="create_songplays_table",
        postgres_conn_id="redshift",
        sql=create_query.songplays_table
    )

create_tables_dag = create_tables_dag()