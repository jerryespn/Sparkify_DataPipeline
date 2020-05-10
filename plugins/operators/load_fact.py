from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id = "redshift",
                 table = "",
                 sql = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        """
        Data load into fact tables from events and songs staging
        :param conn_id -> airflow connection to redshift
        :param table -> target table located in redshift
        :param sql -> sql command
        """
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Starting to Load Data into redshift table: {self.table}")
        load_sql = f"INSERT INTO {self.table} ({self.sql})"
        redshift.run(load_sql)
        self.log.info(f"Success: {self.task_id} loaded.")