from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql_query="",
                 insert_table="songplays",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query.format(kwargs["ds"])
        self.insert_table = insert_table
        self.execution_date = kwargs['ds']

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {} where trunc(start_time) = {}".format(self.table), self.execution_date)

        self.log.info("Loading data to destination Redshift table")
        insert_query = f"insert into {self.insert_table}\n{self.sql_query}"
        redshift.run(insert_query, autocommit=True)
