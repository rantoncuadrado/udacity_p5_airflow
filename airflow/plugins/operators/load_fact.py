
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    """
    Loads facts table (songplays). 
    Parameters: 
        redshift_conn_id: The Redshift connection id
        sql_query: a query to execute to get rows to populate the table with
        table_name: the table name to be populated
        truncate_table: Boolean indicating if the table should be emptied before adding the rows obtained from sql_query    
    """

    truncate_sql = """
    TRUNCATE TABLE {};
    """
        
    insert_sql = """
    INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent) {};
    """
        
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
        
        if self.truncate_table:
            self.log.info(f"RAUL PROJECT: About to truncate table {self.table_name}")
            redshift_hook.run(LoadFactOperator.truncate_sql.format(self.table_name))
            
        self.log.info(f"RAUL PROJECT: About to load fact table {self.table_name}. Query: {LoadFactOperator.insert_sql.format(self.sql_query)}")
        redshift_hook.run(LoadFactOperator.insert_sql.format(self.sql_query))
        self.log.info(f"RAUL PROJECT: Loaded Fact table {self.table_name}.")
  