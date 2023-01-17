
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id,
                 s3_bucket,
                 s3_key,
                 s3_json,
                 aws_key,
                 aws_secret,
                 *args, **kwargs):
                 
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 #*args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_json = s3_json
        self.aws_key = aws_key
        self.aws_secret = aws_secret
       
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
       
        self.log.info('StageToRedshiftOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Setting path to S3 data")
        if self.table == 'staging_events':
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            
        if self.table == 'staging_songs':
            s3_path = "s3://{}/".format(self.s3_bucket)
           
        formatted_sql = """
                        COPY {}
                        FROM {}
                        ACCESS_KEY_ID {}
                        SECRET_ACCESS_KEY {}
                        JSON {}
                        """.format(self.table, s3_path, self.aws_key, self.aws_secret, self.s3_json)
        
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(formatted_sql)

        self.log.info("Data copied from S3 to Redshift")