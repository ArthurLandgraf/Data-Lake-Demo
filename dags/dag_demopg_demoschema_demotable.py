# =============================================================================
# Unica parte que precisa editar quando criar uma nova DAG
# Editar o path do arquivo que contem os dados da tabela
# =============================================================================

# dict de parametros construido para a tabela especifica
# editar apenas para apontar para o arquivo correto a ser usado nesta DAG
from helpers.tabelas.demopg.demoschema_demotable import parametros_tabela

# =============================================================================
# Dependencias gerais (nao precisa editar)
# =============================================================================

from datetime import datetime, timedelta

# =============================================================================
# Dependencias do Airflow (editar conforme necessidade de outros operadores)
# =============================================================================

from airflow import models
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.operators.python_operator import PythonOperator

# =============================================================================
# Dependencias especificas da DAG (nao precisa editar)
# =============================================================================

# importando a funcao que envia alertas no slack
from helpers.slack_alert import slack_alert

# importando a funcao que vai processar os dados ao chegarem no bucket
# e antes de serem importados ao biquery
from helpers.process_data import process_data

# dict de todas as conexoes, para novas fontes atualizar la primeiro
# nao precisa editar
from helpers.connections import connections

# dict de todos as tabelas e seus schedules
from helpers.schedule import schedule

# =============================================================================
# Salvando as variaveis que serao utilizadas (nao precisa editar)
# =============================================================================

staging_bucket = connections['staging_bucket']
id_projeto = connections['id_projeto']
banco_origem = parametros_tabela['banco_origem']
conn_id = connections[banco_origem]
nome_tabela = parametros_tabela['tabela']
base_task_id = parametros_tabela['task_id']
query = parametros_tabela['query']
schema_origem = parametros_tabela['schema_origem']
schema_destino = parametros_tabela['schema_destino']
colunas = parametros_tabela['colunas']
key_schedule= banco_origem + "_" + schema_origem + "_" + nome_tabela
sch_interval = schedule[key_schedule]

ontem = datetime(2023, 10, 18)

# # caso seja necessario deixar o start_date dinamico
# datetime.combine(
#     datetime.today() - timedelta(1),
#     datetime.min.time()
#     )

# =============================================================================
# Inicio da DAG
# =============================================================================

default_args = {
    'start_date': ontem,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_alert,
    'on_success_callback': slack_alert,
}

with models.DAG(
    base_task_id,
    schedule_interval=sch_interval,
    default_args=default_args,
    catchup=False
) as dag:

    transfer_to_gcs = PostgresToGCSOperator(
        task_id='{}_extract'.format(base_task_id),
        sql=query,
        bucket=staging_bucket,
        filename='output/output_{}_{}.csv.gz'.format(schema_origem, nome_tabela),
        postgres_conn_id=conn_id,
        export_format='csv',
        field_delimiter=',',
        gzip=True,
        execution_timeout=timedelta(minutes=10),
    )

    data_processing = PythonOperator(
        task_id='{}_transform'.format(base_task_id),
        python_callable=process_data,
        op_kwargs={
            'nome_tabela': nome_tabela,
            'schema_origem': schema_origem,
            'staging_bucket': staging_bucket,
            },
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id='{}_load'.format(base_task_id),
        bucket=staging_bucket,
        source_objects=['output/processed2_{}_{}.csv'.format(schema_origem, nome_tabela)],
        destination_project_dataset_table='{}.{}.{}'.format(
            id_projeto,
            schema_destino,
            nome_tabela
        ),
        schema_fields=colunas,
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        execution_timeout=timedelta(minutes=10),
    )
    
    delete_gcs_files = GCSDeleteObjectsOperator(
        task_id='{}_clean_bucket'.format(base_task_id),
        bucket_name=staging_bucket,
        objects=[
            'output/processed_{}_{}.csv'.format(schema_origem, nome_tabela), 
            'output/output_{}_{}.csv.gz'.format(schema_origem, nome_tabela),
            ],
        execution_timeout=timedelta(minutes=10),
)

    transfer_to_gcs >> data_processing >> load_to_bigquery >> delete_gcs_files