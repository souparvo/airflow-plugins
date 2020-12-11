
def insert_metatable():
    return """
    INSERT INTO TABLE default.metatable_dev VALUES 
    ('{{ params.schema }}', '{{ params.table }}', {{ ti.xcom_pull(key='hive_res', task_ids=params.count_inserts)[0][0] }}, current_timestamp(), '{{ params.type }}');
    """