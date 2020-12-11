# airflow-plugins

Plugins for Apache Airflow. Please see [Airflow official documentation - Plugins](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html)
to augment or use plugins.

Placing the contents of this repo inside `$AIRFLOW_HOME/plugins` folder and reloading the webserver will have the plugins avaliable to your Airflow DAGs to use. 

`funtions`and `queries` folder includes files to enable functions to be accessible to all DAGs 

## es_plugin

Plugin to interact with Elasticsearch.

### Requirements

Needs python package [elasticsearch-py](https://elasticsearch-py.readthedocs.io/en/master/index.html). Install package on Airflow's python 
virtualenv on all Airflow workers and webserver. It is recommended tot install version of elasticsearch you are interacting with as the 
version of the package.

## data_mon_plugin

Plugin to save record count to elasticsearch document using either Presto or Hive. 

Document format inserted on Elasticsearch:

```python
doc = {
    'warehouse': 'hive', # name of warehouse where records were inserted
    'schema': self.schema, # name of the schem where records were inserted
    'table': self.table, # name of table where records were inserted
    'records': the_records, # result from count
    'insert_ts': datetime.utcnow(), # UTC timestamp when records are counted and inserted
    'dag_id': context['dag'].dag_id, # DAG ID responsible for insert
    'execution_date': context['ts'], # execution date responsible for insert
    'dag_run_id': context['run_id'] # DAG run ID
}
```