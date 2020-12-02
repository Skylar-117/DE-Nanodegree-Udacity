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
                 redshift_conn_id="redshift",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        Execute function for checking data quality
        """

        # Connections to Redshift through Airflow PostgresHook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Check if there are records or not in the table to make sure that the table is not empty
        # Iterate through all tables to check emptyness
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")        
            if records is None or len(records[0]) < 1:
                self.log.error(f"Table '{table}' returned no records")
                raise ValueError(f"Data quality check failed. Table '{table}' returned no records")
            num_records = records[0][0]
            if num_records == 0:
                self.log.error(f"No records present in destination table '{table}'")
                raise ValueError(f"No records present in destination table '{table}'")
            self.log.info(f"Passed data quality check on table {table} with {num_records} records")