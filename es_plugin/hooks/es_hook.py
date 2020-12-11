from airflow.hooks.base_hook import BaseHook

from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import json
from uuid import uuid4

class ESHook(BaseHook):
    """Hook using Eleasticsearhc python client 
    from https://elasticsearch-py.readthedocs.io/en/master/index.html

    :param es_conn_id: connection with elasticsearch node (type http)
    :type es_conn_id: str

    """
    
    def __init__(self, es_conn_id='es_default'):
        self.es_conn_id = es_conn_id
        self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn is not None:
            self.close_conn()

    def get_conn(self):
        """
        Returns an Elasticesearch connection object
        """
        if self.conn is None:
            params = self.get_connection(self.es_conn_id)
            pasv = params.extra_dejson.get("passive", True)
            if params.login != '':
                self.conn = Elasticsearch([':'.join([params.host, str(params.port)])], http_auth=(params.login, params.password))
            else:
                self.conn = Elasticsearch([':'.join([params.host, str(params.port)])])

        return self.conn


    def close_conn(self):
        """
        Closes the connection. An error will occur if the
        connection wasn't ever opened.
        """
        conn = self.conn
        conn.transport.connection_pool.close()
        self.conn = None

    def search(self, index, query):
        """
        Searches an index using a query in JSON format as a dict

        :param index: index to search
        :type index: str
        """
        es = self.get_conn()

        return es.search(index=index, body=json.dumps(query))

    def insert_doc(self, index, document, doc_type='_doc', doc_id=None):
        """
        Inserts documents to elasticsearch index

        :param index: index to insert (templated)
        :type index: str
        :param document: document to insert into elasticsearch
        :type document: dict
        :param doc_type: type of document
        :type doc_type str:
        :param doc_id: id for the document
        :type doc_id: str
        """
        es = self.get_conn()
        res = es.index(index=index, body=document, doc_type=doc_type, id=doc_id)
        self.log.info("inserted document")

        return res

