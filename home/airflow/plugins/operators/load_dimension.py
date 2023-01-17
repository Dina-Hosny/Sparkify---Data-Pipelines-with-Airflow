from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 table = '',
                 redshift_conn_id = '',
                 sql = '',
                 action = 'truncate',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
       
        # Map params here       
        self.table = table        
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.action = action
        
    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.action == 'truncate':
            self.log.info("Truncating table {}".format(self.table))
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info("Inserting data from staging table into {} table".format(self.table))
        custom_sql = "INSERT INTO {} {}".format(self.table, self.sql)
        redshift.run(custom_sql)        
        
        self.log.info("Success: Inserting values on {}, {} loaded.".format(self.table))#, self.task_id))
        