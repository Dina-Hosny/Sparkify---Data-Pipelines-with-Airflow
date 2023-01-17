from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables,
        
    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for tbl in range(len(self.tables)):
            table = list(self.tables.keys())[tbl][0]
            field = list(self.tables.values())[tbl][1]
            
            # Quality check 1 - check that dimension tables have rows
            custom_sql = f"SELECT Count(*) FROM {table}"
            rows = redshift.get_first(custom_sql) 
            self.log.info(f'Table: {table} has {rows} rows')
            
            # Quality check 2 - check that key fields dont have null entries
            custom_sql = f"SELECT Count(*) FROM {table} where {field} IS NULL"
            rows = redshift.get_first(custom_sql) 
            self.log.info(f'Field: "{field}" in table: {table} has {rows} NULL rows')