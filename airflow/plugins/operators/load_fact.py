
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#00FF00'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_name='',
                 truncate_table='',
                 sql_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.truncate_table = truncate_table
        self.sql_query = sql_query

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if self.truncate_table == True:
            self.log.info(f"RAUL PROJECT: About to truncate table {self.table_name}.")
            redshift_hook.run(f"TRUNCATE TABLE {self.table_name}")
            
        self.log.info(f"RAUL PROJECT: About to load fact table {self.table_name}.")
        self.log.info(self.sql_query)
        redshift_hook.run(self.sql_query)
        self.log.info(f"RAUL PROJECT: Loaded Fact table {self.table_name}.")
  