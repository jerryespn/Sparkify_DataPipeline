from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_dimdata = (
        """
        INSERT INTO {table} (
            {fields}
        )
        {load_dimension}
        """
    )

    @apply_defaults
    def __init__(self,
                 conn_id = "redshift",
                 table = "",
                 fields = "",
                 load_dimension = "",
                 append_only = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.fields = fields
        self.load_dimension = load_dimension
        self.append_only = append_only

    def execute(self, context):
        """
        Data load into dimension tables from staging events and staging songs
        :param conn_id -> connection to redshift
        :param table -> table at reshift cluster
        :param -> sql -> query to insert data
        """
        redshift = PostgresHook (self.conn_id)

        if self.append_only == False:
            self.log.info(f"Truncating {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        
        insert_data = LoadDimensionOperator.insert_dimdata.format(
            table = self.table,
            fields = self.fields,
            load_dimension = self.load_dimension
        )

        self.log.info('Loading Dimension Data')
        redshift.run(insert_data)
