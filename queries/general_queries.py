
def insert_metatable():
    """SQL query to insert records from table insert into a table on a DB
    """

    return """
    INSERT INTO TABLE {{ params.target_schema }}.{{ params.target_table }} VALUES 
    ('{{ params.schema }}', '{{ params.table }}', {{ ti.xcom_pull(key='hive_res', task_ids=params.count_inserts)[0][0] }}, current_timestamp(), '{{ params.type }}');
    """