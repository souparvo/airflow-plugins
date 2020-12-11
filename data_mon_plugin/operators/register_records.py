from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.hooks.presto_hook import PrestoHook
from es_plugin.hooks.es_hook import ESHook

from datetime import datetime

class RecordHiveOperator(BaseOperator):
    """Operator do insert a registe for records on elasticsearch using 
    the ESHook.

    Arguments:
        es_conn [str] -- connection ID for elasticsearch node
        schema [str] -- Hive schema where the (templated)
        table [str] -- Hive table where the records were inserted (templated)
        records [str] -- Number of lines inserted, should be string to be able use x_com (templated)
        index [str] -- index to insert into elasticsearch (templated)
    """

    template_fields = ('schema', 'table', 'records_query', 'records', 'index')

    ui_color = '#2980b9'
    
    @apply_defaults
    def __init__(
            self,
            es_conn,
            schema,
            table,
            records_query,
            index='metatable',
            records=None,
            query_engine='hive',
            query_engine_conn_id='hiveserver2_default',
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.es_conn = es_conn
        self.schema = schema
        self.table = table
        self.records = records
        self.index = index
        self.records_query = records_query
        self.query_engine = query_engine
        self.query_engine_conn_id = query_engine_conn_id

    def execute(self, context):
        """Inserts a record on the metadata index to register all the
        records inserted into the tables.

        """
        # Schema of the record register - metatable
        the_records = self.get_records()

        doc = {
            'warehouse': 'hive',
            'schema': self.schema,
            'table': self.table,
            'records': the_records,
            'insert_ts': datetime.utcnow(),
            'dag_id': context['dag'].dag_id,
            'execution_date': context['ts'],
            'dag_run_id': context['run_id']
        }

        with ESHook(es_conn_id=self.es_conn) as es:
            self.log.info("Inserting document into index %s" % self.index)
            res = es.insert_doc(index=self.index, document=doc)
            self.log.info("Inserted document, result: %s" % res)
        # do xcom push
        context['ti'].xcom_push('records', the_records)

        return res

    def get_records(self):
        """Executes a query to obtain a count of records on a table

        Returns:
            int -- quantity of records from a count query
        """

        if self.query_engine == 'hive': 
            hook = HiveServer2Hook(self.query_engine_conn_id)
            
        elif self.query_engine == 'presto': 
            hook = PrestoHook(self.query_engine_conn_id)
        
        res = hook.get_records(self.records_query)

        if len(res) > 1:
            raise
        else:
            return res[0]

