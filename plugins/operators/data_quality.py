from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 column="userid",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.column = column

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        num_rows = records[0][0]

        col_not_null = redshift.get_records(f"SELECT COUNT(*) FROM {self.table} where {self.column} is not null")
        col_not_null = col_not_null[0][0]

        self.log.info("Data quality checks")

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        if num_rows < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        if col_not_null < num_rows:
            raise ValueError(f"Data quality check failed. column {self.column} has empty values")