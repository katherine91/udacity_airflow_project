from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            FORMAT AS JSON 'auto';
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="staging_songs",
                 s3_bucket="katya-bucket-redshift2",
                 s3_key="song-data/A",
                 region="us-west-2",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.aws_credentials_id = aws_credentials_id
        self.execution_date = kwargs["ds"]

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # for song table delete data from table to prevent duplicates, log data will append depending on dag run date
        if self.table == "staging_songs":
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        if self.table == "staging_events":
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {} where trunc(TIMESTAMP 'epoch' + ts/1000 * interval '1 second') = {}".format(self.table), self.execution_date)

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region
        )
        self.log.info("Loading data to destination Redshift table")
        redshift.run(formatted_sql, autocommit=True)







