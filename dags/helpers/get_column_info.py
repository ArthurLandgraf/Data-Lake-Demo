# =============================================================================
# PostgreSQL
# =============================================================================

import re

def postgres_to_bigquery_type(pg_type):
    """
    Convert PostgreSQL data type to BigQuery data type
    """
    # An expanded mapping from PostgreSQL to BigQuery data types
    type_mapping = {
        'uuid': 'STRING',
        'int2': 'INT64',
        'int4': 'INT64',
        'int8': 'INT64',
        'smallserial': 'INT64',
        'serial': 'INT64',
        'bigserial': 'INT64',
        'money': 'FLOAT64',
        'decimal': 'NUMERIC',
        'numeric': 'NUMERIC',
        'real': 'FLOAT64',
        'double precision': 'FLOAT64',
        'float8': 'FLOAT64',
        'float4': 'FLOAT64',
        'float': 'FLOAT64',
        'smallint': 'INT64',
        'integer': 'INT64',
        'bigint': 'INT64',
        'character varying': 'STRING',
        'varchar': 'STRING',
        'character': 'STRING',
        'char': 'STRING',
        'text': 'STRING',
        'bytea': 'BYTES',
        'timestamp': 'TIMESTAMP',
        'timestamp without time zone': 'TIMESTAMP',
        'timestamp with time zone': 'TIMESTAMP',  # Adjust based on how you handle timezones in BigQuery
        'date': 'DATE',
        'time': 'TIME',
        'time without time zone': 'TIME',
        'time with time zone': 'TIME',
        'interval': 'INTERVAL',
        'bool': 'BOOL',
        'boolean': 'BOOL',
        'json': 'STRING',
        'jsonb': 'STRING',
        'array': 'ARRAY',
        'cidr': 'STRING',
        'inet': 'STRING',
        'macaddr': 'STRING',
        # Add more mappings as needed
    }
    
    return type_mapping.get(pg_type, 'STRING')

def append_bigquery_types(column_details):
    """
    Append BigQuery data type to column details based on PostgreSQL data type
    """
    for column in column_details:
        if (column['name'] == 'Id') or ( 'Id' in column['name']):
            pg_type = column['type_pg']
            bq_type = 'STRING'
            column['type_bq'] = bq_type
        else:
            pg_type = column['type_pg']
            bq_type = postgres_to_bigquery_type(pg_type)
            column['type_bq'] = bq_type

def extract_columns_from_ddl(ddl_string):
    # Split the string into lines
    lines = ddl_string.split("\n")
    
    columns = []
    
    for line in lines:
        # Regular expression to extract column details
        match = re.match(r'^\s+"(\w+)"\s(\w+)[\(\s](NOT NULL)?', line)
        if match:
            column_name, column_type, column_rule = match.groups()
            columns.append({
                'name': column_name,
                'type_pg': column_type,
                'mode': 'NULLABLE' if column_rule else 'NULLABLE'
            })
    
    append_bigquery_types(columns)
    
    return columns
