from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Executes a COPY command to load files from s3 to Redshift staging tables. It needs 
        aws_conn_id: Amazon Web Services connection (where the buckets are)
        s3_bucket and s3_key 
        file_format, log_file
        region
          
        redshift_conn_id: Redshift connection
        table: The name of the table where we'd like to copy the bucket content
    """
    
    ui_color = '#ff0000'

    copy_query = " COPY {} \
    FROM '{}' \
    ACCESS_KEY_ID '{}' \
    SECRET_ACCESS_KEY '{}' \
    FORMAT AS json '{}'; \
    "
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults)
                 table = '',
                 s3_bucket = '',
                 s3_key = '',
                 region = '',
                 redshift_conn_id = '',
                 aws_conn_id = '',
                 log_file = '',
                 file_format = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.log_file = log_file
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        self.log.info(f"RAUL PROJECT: Created AWS Hook {self.aws_conn_id}")
        credentials = aws_hook.get_credentials()      
        self.log.info(f"RAUL PROJECT: Got credentials {credentials}")
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"RAUL PROJECT: Extract table {self.table} from : {s3_path}")    
        
        if self.log_file != '':
            self.log_file = "s3://{}/{}".format(self.s3_bucket, self.log_file)
            copy_query = self.copy_query.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.log_file)
        else:
            copy_query = self.copy_query.format(self.table, s3_path, credentials.access_key, credentials.secret_key, 'auto')
        
        
        self.log.info(f"RAUL PROJECT: About to execute copy query {copy_query}")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        redshift_hook.run(copy_query)
        self.log.info(f"RAUL PROJECT: Copied Table {self.table} ")

