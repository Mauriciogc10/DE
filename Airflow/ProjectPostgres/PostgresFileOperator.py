from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import smtplib
import ssl
from email.message import EmailMessage
from airflow.models import Variable


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

def execute(self, context):
    if self.operation == "write":
        #escribir en la db
        self.writeToDb()

    elif self.operation == "read":
        #leer la db
        self.readFromDb()

def writeToDb(self):
    self.postgres_hook.bulk_load(config.get('table_name'),'file.tsv')

def readFromDb(self) :
    # read from db with a SQL query
    conn = self-postgres_hook.get_conn ()
    cursor = conn. cursor()
    cursor.execute(self.config.get("query"))

    if data: #s1 nay resuctados de mi query
        #send mail
        email_from ="pensar.coding@gmail.com"
        passw = Variable.get ("passw_email")
        email_to = "pensar.coding@gmail.com"
        title = "ALERTA ! Items con demasiadas ventas"
        body = """
        Hemos detectado nuevos items con demasiadas ventas:{} """. format (data)

        email = EmailMessage ()
        email[ 'From'] = email_from
        email( 'To*] = email_to
        emaill 'Subject'] = title
        email. set_content

        context = ssl. create_default_context() 
        with smtplib.SMTP_SSL('smtp.gmail.com' ,465, context=context) as smtp:
             smtp. login (email_from, passw)
             smtp.sendmail(email_from, email_to, email.as_string())