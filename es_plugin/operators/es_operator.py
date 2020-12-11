from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.hive_hooks import HiveServer2Hook
from es_plugin.hooks.es_hook import ESHook

import json
from datetime import datetime

class ESInsertOperator(BaseOperator):
    """Operator do insert a document into elasticsearch using 
    the ESHook.

    Arguments:
        es_conn [str] -- connection ID for elasticsearch node
        document [str] -- document to insert into elasticsearch (templated). Can
        also be a path to a .json file
        index [str] -- index to insert into elasticsearch (templated)
    """

    template_fields = ('document', 'index')
    template_ext = ('.json',)

    ui_color = '#2980b9'
    
    @apply_defaults
    def __init__(
            self,
            es_conn,
            document,
            index,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.document = document
        self.es_conn = es_conn
        self.index = index

    def execute(self, context):
        """Inserts a document into elastisearch using the provided document

        """
        doc = json.loads(self.document)

        with ESHook(es_conn_id=self.es_conn) as es:
            self.log.info("Inserting document into index %s" % self.index)
            res = es.insert_doc(index=self.index, document=doc)
            self.log.info("Inserted document, result: %s" % res)
        return res