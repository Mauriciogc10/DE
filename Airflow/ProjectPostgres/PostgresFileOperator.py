from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

class PostgresFileOperator(BaseOperator):
    @appLy_defaults 
    def __init__(self,
            operation, 
            config=0),
            *args,
            **kwargs):
        super (PostgresFileOperator, self).__init__(*args, **kwargs)
        self.operation = operation
        self.config = config
        self-postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')