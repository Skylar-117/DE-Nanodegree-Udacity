from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table="",
                 redshift_conn_id="redshift",
                 select_sql="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql
        self.append =  append

    def execute(self, context):
        """
        Execute function for running insert sql syntax to insert data to dimension table
        """

        # Connections to Redshift through Airflow PostgresHook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Delete table if append==False to avoid existing table
        if not self.append:
            self.log.info(f"Deleting '{self.table}' table...")
            redshift_hook.run("DELETE FROM {table}".format(table=self.table))
        
        # If append==True, then execute insert sql statement
        self.log.info(f"Inserting data into '{self.table}' dimension table...")
        insert_sql = """
            INSERT INTO {table}
            {insert_sql};
        """.format(table=self.table, insert_sql=self.select_sql)
        redshift_hook.run(insert_sql)
        self.log.info(f"Successfully inserted data into '{self.table}' table!")