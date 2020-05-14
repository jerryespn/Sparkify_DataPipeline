from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql_data = ("""
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{key_id}'
        SECRET_ACCESS_KEY '{access_key}'
        FORMAT AS {file_format} 
    """)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 file_format = "",
                 append_data = False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_crendentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.append_data = append_data

    def execute(self, context):
        self.log.info('Connecting to AWS')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info('Connecting to redshift')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        # Clearing table if there is data
        if self.append_data == False:
            self.log.info('Clearing table')
            redshift.run(f"DELETE FROM {self.table}")

        # Connecting to S3 to get te data 
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        # Copying data from S3 to Redshift
        self.log.info(f"Copying data from {s3_path} to Redshift")
        copy_from_s3 = StageToRedshiftOperator.copy_sql_data.format(
            table = self.table,
            s3_path = s3_path,
            key_id = credentials.access_key,
            access_key = credentials.secret_key,
            file_format = self.file_format)
        redshift.run(copy_from_s3)
