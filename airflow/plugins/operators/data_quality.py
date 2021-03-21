from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Executes quality checks for created tables. 
    Quality Checks:
        1. Checks if the table is empty
        2. Checks if there are null values in given columns
    
    Parameters: 
        redshift_conn_id: The Redshift connection id
        table_names: a list of table names
        table_pks: a list of columns for each one of the above tables to check for null rows. Usually PKs    
    """

    check_empty_sql = """
    SELECT COUNT(*) FROM {};
    """
    
    check_null_pk_sql = """
    SELECT COUNT(*) FROM {} WHERE {} is null;
    """
    
    ui_color = '#ffff00'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_names='',
                 table_pks='',
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names
        self.table_pks = table_pks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"RAUL PROJECT: table_names is {self.table_names} to see if it is empty")
        self.log.info(f"RAUL PROJECT: table_pks is {self.table_pks} to see if it is empty")
        
        for table,pk in zip(self.table_names,self.table_pks):
            query_empty = DataQualityOperator.check_empty_sql.format(table)
            query_pk = DataQualityOperator.check_null_pk_sql.format(table,pk)
            connection = redshift_hook.get_conn()
            cursor = connection.cursor()
            
            # EMPTY TABLE CHECK
            self.log.info(f"RAUL PROJECT: CHECKING {table} to see if it is empty")
            self.log.info(f"RAUL PROJECT: About to execute this query -> {query_empty}")
            cursor.execute(query_empty)
            records = cursor.fetchall()
            self.log.info(f"RAUL PROJECT: Number of rows in {table} -> {records[0][0]}")
            
            if records[0][0] < 1 :
                error_message = "TABLE {} has no records.".format(table)
                self.log.error(f"RAUL PROJECT FAIL: {error_message}")
            
            else:
                self.log.info(f"RAUL PROJECT: TABLE {table} IS POPULATED")
            
            # NULL PKs CHECK
            self.log.info(f"RAUL PROJECT: CHECKING {table} for NULL PKs")
            self.log.info(f"RAUL PROJECT: About to execute this query -> {query_pk}")
            cursor.execute(query_pk)
            records = cursor.fetchall()
            self.log.info(f"RAUL PROJECT: Number of null PKs in {table} -> {records[0][0]}")
            
            if records[0][0] < 1 :
                self.log.info(f"RAUL PROJECT: TABLE {table} has no null {pk} rows")            
            else:
                error_message = "TABLE {} has {} records = null.".format(table, pk)
                self.log.error(f"RAUL PROJECT FAIL: {error_message}")