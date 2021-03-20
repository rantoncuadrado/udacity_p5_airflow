from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    truncate_sql = """
    TRUNCATE TABLE {};
    """
    insert_sql = """
    INSERT INTO {} {};
    """
    
    ui_color = '#0000EE'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_query='',
                 table_name='',
                 truncate_table='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.truncate_table = truncate_table

        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f"RAUL PROJECT: About to truncate table {self.table_name}")
            #redshift_hook.run(f"TRUNCATE TABLE {self.table_name}")
            #trying out this way
            redshift_hook.run(LoadDimensionOperator.truncate_sql.format(self.table_name))
        
        self.log.info(f"RAUL PROJECT: About to load {self.table_name} Table.")
        self.log.info(self.sql_query)
        redshift_hook.run(LoadDimensionOperator.insert_sql.format(self.table_name, self.sql_query))
        self.log.info(f"RAUL TABLE: Dimension table {self.table_name} loaded.")
        
                
        
        