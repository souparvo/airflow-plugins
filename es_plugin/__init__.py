from airflow.plugins_manager import AirflowPlugin
from es_plugin.hooks.es_hook import ESHook
from es_plugin.operators.es_operator import ESInsertOperator

class ESPlugin(AirflowPlugin):
    name = 'es_plugin'
    hooks = [ESHook]
    operators = [ESInsertOperator]
    # Not used - part of the interface
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

