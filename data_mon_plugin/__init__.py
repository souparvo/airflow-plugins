from airflow.plugins_manager import AirflowPlugin
from data_mon_plugin.operators.register_records import RecordHiveOperator

class DataMonPlugin(AirflowPlugin):
    name = 'data_mon_plugin'
    hooks = []
    operators = [RecordHiveOperator]
    # Not used - part of the interface
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
