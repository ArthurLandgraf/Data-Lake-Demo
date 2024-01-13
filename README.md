# **Data Lake Demo com Airflow (via Google Cloud Composer)**
---

O código a seguir representa uma demonstração de como configurar um data lake básico e com ferramentas open-source (com excessão dos serviços da Google Cloud). Essa demonstração tem seu valor comprovado, pois todo o código é baseado em uma implementação real de um trabalho onde tive que criar um date lake do zero sozinho.

**Ferramentas Utilizadas**
* **Cloud:** Google Cloud Platform
* **Armazenamento:** Google Big Query
* **Orquestramento de ingestões:** Google Cloud Composer (Airflow)
* **Processamento de dados:** Python Polars (Rust)

**Informações específicas do ecossistema**
* **GCP Project:** demo-arthur
* **Instância do Composer:** demo-composer
* **Bucket para armazenamento temporário na pipeline (staging):** demo-staging-bucket
* **Bucket para as DAGs do Airflow:** demo-dag-bucket

# Organização do Airflow
---

Tudo que se refere a organização do Airflow pode ser encontrado dentro do Bucket para as DAGs.

**Conceitos básicos**
* Cada DAG representa uma tabela
* A pasta ```helpers``` armazena todas as funções ou scripts cross-DAGs e as outras pastas
* Dentro da pasta ```helpers.tabelas``` deve ser criada uma pasta separada para cada fonte de dados
* A pasta de cada fonte armazena arquivos em ```.py``` para cada tabela

# Connections e Schedule
---

Os arquivos ```connections.py``` e ```schedule.py``` estão localizados dentro da pasta ```helpers``` e tem como objetivo centralizar as informações de ```conn_id``` e as definições de horários que cada DAG vai rodar seguindo o formato cron.

** Conteúdo da ```connections.py```:
```python
# =============================================================================
# No dicionario abaixo devem ser preenchidas todas as conexoes existentes
# 1- os valores do dict devem ser iguais as conexoes registradas no airflow ui
# 2- as chaves do dict devem ser iguais aos nomes dos bancos descritos em
# cada arquivo de tabela em 'helpers.tabelas', na variavel 'banco_origem'
# =============================================================================

# id_projeto se refere ao id do projeto na google cloud

# staging_bucket se refere ao id do bucket de staging (nao o de dags) que foi
# criado para servir como deposito temporario de arquivos que serao movidos
# ao bigquery

connections = {
    'id_projeto': 'demo-arthur',
    'staging_bucket': 'demo-staging-bucket',
    'demopg': 'postgres_interno',
    'slack_conn': 'slack_app',
}

```

** Conteúdo da ```schedule.py```:
```python
# slots para tabelas mais leves
slot_1 = '5 3 * * 0-6'
slot_2 = '10 3 * * 0-6'
slot_3 = '15 3 * * 0-6'
slot_4 = '20 3 * * 0-6'
slot_5 = '25 3 * * 0-6'
slot_6 = '30 3 * * 0-6'

# slots para tabelas mais pesadas
slot_7 = '40 3 * * 0-6'
slot_8 = '50 3 * * 0-6'
slot_9 = '0 4 * * 0-6'

schedule = {
    'demopg_demoschema_demotable': slot_7,
    ...
}

```


# Arquivos da tabela
---

Os arquivos de cada tabela, localizados em ```helpers.tabelas.fonte``` contém as informações básicas de cada tabela e devem ser nomeados no formato ```schemaorigem_nometabela``` sempre respeitando as mesmas regras de capitalização da fonte de dados.

**Conteúdo dos arquivos de tabela**
* ```banco_origem``` [Str] Nome da fonte de origem
* ```schema_origem``` [Str] Nome da schema no banco de origem
* ```schema_destino``` [Str] Nome do conjunto de dados no Big Query
* ```tabela``` [Str] Nome da tabela exatamente igual ao banco de origem
* ```query``` [Str] Consulta em SQL que será executada para a extração dos dados
* ```task_id``` [Str] Identificador que vai ser mostrado na UI e Logs do Airflow *
* ```colunas``` [List][Dict] Lista em que cada item é um dicionário que, por sua vez, representa uma coluna
    * Conteúdo do dicionário:
    * ```'name'```: Nome da coluna
    * ```'type'```: Tipo da coluna no formato do Big Query
    * ```'mode'```: Definição de valores nulls são aceitos ou não

Todas essas variáveis são armazenadas em um dicionário ```parametros_tabela``` como mostrado no código a seguir:

```python
parametros_tabela = {
    'tabela': tabela,
    'banco_origem': banco_origem,
    'schema_origem': schema_origem,
    'schema_destino': schema_destino,
    'query': query,
    'task_id': 'etl_{}_{}_{}'.format(
        banco_origem, 
        schema_origem, 
        tabela
    ),
    'colunas': colunas
}
```

\* *```task_id``` de ingestões simples devem seguir o formato ```etl_fonte_schemaorigem_tabela```, mas para outros tipos de ingestão o prefixo pode ser alterado para algo que faça sentido, por exemplo transformações para o DW podem começar com ```dw_``` sempre mantendo o mesmo padrão de sufixo.*

**Exemplo de um arquivo de tabela**
```python
from helpers.get_column_info import extract_columns_from_ddl

# =============================================================================
# Variaveis que precisam ser definidas manualmente
# =============================================================================

# nome do banco que a tabela pertence, deve ser igual ao nome preenchido em
# helpers.connections, pois ele usa a string para localizar o 'conn_id'
banco_origem = 'demopg' 

# nome da tabela, deve ser exatamente igual
tabela = 'demotable'

# nome da schema no banco original
schema_origem = 'demoschema' 

# nome do conjunto de dados no bigquery
schema_destino = 'demodataset' 

# query que lista todas as colunas a serem extraidas
# evitar SELECT * para transparencia
query = '''
SELECT "Id", "UserId", "TypeId", "CarId", "LocationId", "OtherId"
FROM demoschema."demotable";
'''

# adicionando a coluna de created_at_datalake fixa (nao precisa editar)
query = query.replace('SELECT', 'SELECT current_timestamp as created_at_datalake,')

# ddl usada para extrair com regex os tipos e modos da query
# para achar a ddl, basta abrir o dbeaver, entrar na tabela, DDL no menu esquerdo
# copiar somente o CREATE TABLE (sem CREATE INDEX) que fica ate o primeiro ';'
# para mais informacoes da funcao de extrair veja helpers.get_column_info
# recomendo sempre rodar esse bloco antes e ver se puxou todas as colunas
ddl = '''
CREATE TABLE demoschema."demotable" (
	"Id" uuid NOT NULL,
	"UserId" int4 NOT NULL,
	"TypeId" int4 NOT NULL,
	"CarId" int4 NULL,
	"LocationId" uuid NOT NULL,
	"OtherId" uuid NOT NULL,
);
'''

# lista em que cada item e um dicionario representando uma coluna
# 'nome' = nome da coluna 
# 'type' = tipo de dados aceito na coluna **
# 'mode' = define se essa coluna aceita valores null **
# ** para consultar as opcoes possiveis, veja os comentarios ao final do script
# nao precisa editar nada abaixo, desde que a ddl esteja correta acima
# para entender a funcao, ver documentacao em helpers.get_column_info.py

colunas_bruto = extract_columns_from_ddl(ddl)

# renomeando e mantendo somente o necessario no dict para a task de load no bq
colunas = [{'name': 'created_at_datalake', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}]
for col in colunas_bruto:
    nome = col['name']
    tipo = col['type_bq']
    modo = col['mode']
    colunas.append({'name': nome, 'type': tipo, 'mode': modo})

# caso seja necessario input manual
# colunas = [
#     {'name': 'Id', 'type': 'STRING', 'mode': 'REQUIRED'},
#     {'name': 'Descricao', 'type': 'STRING', 'mode': 'NULLABLE'},
#     ]

# =============================================================================
# Dicionário que agrega todas as variáveis e vai ser importado na dag
# =============================================================================

parametros_tabela = {
    'tabela': tabela,
    'banco_origem': banco_origem,
    'schema_origem': schema_origem,
    'schema_destino': schema_destino,
    'query': query,
    'task_id': 'etl_{}_{}_{}'.format(
        banco_origem, 
        schema_origem, 
        tabela
    ),
    'colunas': colunas
}

# =============================================================================
# Opcoes para 'type' e 'mode'
# =============================================================================

# 'type' = tipo de dados aceito na coluna
    # Opcoes de 'type':
    # STRING: Represents a textual string.
    # BYTES: Represents a sequence of bytes.
    # INTEGER: Represents a whole number (64-bit integer).
    # FLOAT: Represents an approximate numeric value with floating point.
    # NUMERIC: Represents a precise numeric value with a fixed point 
    #   (with 38 digits of precision and 9 digits of scale).
    # BOOLEAN: Represents a true or false value.
    # TIMESTAMP: Represents an absolute point in time with microsecond 
    #   precision.
    # DATE: Represents a logical calendar date.
    # TIME: Represents a logical time, independent of any specific date.
    # DATETIME: Represents a year, month, day, hour, minute, second, and 
    #   subsecond.
    # GEOGRAPHY: Represents a geographical point, line, or polygon.
    # ARRAY: Represents a list of zero or more items of a specified data type. 
    #   This isn't specified directly as a type but rather inferred from the 
    #   mode key being set to REPEATED.
    # STRUCT (or RECORD): Represents a list of fields within a data type. 
    #   This type is useful for nested data structures. When specifying a 
    #   STRUCT/RECORD, you will also provide additional field schemas within 
    #   using the fields key.
    # INT64: Equivalent to INTEGER.
    # FLOAT64: Equivalent to FLOAT.
    # BOOL: Equivalent to BOOLEAN.
    # BYTES: Represents a sequence of bytes.
    # BIGNUMERIC: Represents a wide, fixed-point number with 76.76 digits of 
    #   precision and scale.
    # INTERVAL: Represents a duration of time.

# 'mode' = define se essa coluna aceita valores null
    # opcoes para 'mode':
    # NULLABLE: The field allows NULL values.
    # REQUIRED: The field does not allow NULL values.
    # REPEATED: The field is an array of the specified type    

```


# DAGs
---

As DAGs ficam localizadas no diretório anterior ao ```helpers``` soltas mesmo (para a Airflow UI encontrar). Como dito anteriormente, para cada tabela deve haver um arquivo de DAG separado, e o motivo disso é para facilitar o processo de resolver bugs e impedir que um erro em uma tabela bloqueie múltiplas tabelas de serem carregadas. Cada DAG deve ser nomeada seguindo o formato ```nomefonte_schemaorigem_nometabela```.

**Conteúdo das DAGs**
Cada DAG pode ter o seu código específico, mas no geral todas as DAGs devem ter alguns imports em comum:

```python
# dict de parametros construido para a tabela especifica
# editar apenas para apontar para o arquivo correto a ser usado nesta DAG
from helpers.tabelas.demopg.demoschema_demotable import parametros_tabela

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
```

Além dos imports, algumas variáveis em comum devem ser mantidas em todas as DAGs:

```python
ontem = datetime(2023, 10, 18)

default_args = {
    'start_date': ontem,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_alert,
    'on_success_callback': slack_alert,
}
```

A variável ```ontem``` não importa tanto, só é importante ser uma data estática, pois o Airflow contabiliza as DAGs como únicas com base na data de início, caso seja usado, por exemplo, ```datetime.now()``` todos os dias que essa DAG rodar o Airflow vai entender que uma nova DAG foi criada com uma data de início diferente.

As variáveis ```retries``` e ```retry_delay``` podem ser editadas de acordo com a necessidade da DAG.

**Exemplo de uma DAG funcional**
```python
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
```

# Extras
---

Algumas funções são aplicadas em todas as DAGs, ou em DAGs específicas para fontes específicas. Todas as DAGs chamam a ```slack_alert``` que manda notificações no canal #alertas-datalake, e todas as DAGs chamam a ```process_data``` que faz alguns steps de limpeza básicos para não ocorrer erros na ingestão para o BQ.

Por fim, a ```get_column_info``` funciona somente para consultas no PostgreSQL e ajuda a automatizar o processo de gerar a lista de dicionários de colunas para o arquivo de tabela. Ela usa a DDL da tabela para mapear cada coluna e seus tipos na fonte PostgreSQL, converte nos dtypes equivalentes do Big Query e já retorna a lista de dicionários.

**slack_alert.py**
```python
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

# Send slack notification from our failure callback
def slack_alert(context):
    ti = context['ti'] # to get the Task Instance
    task_state = ti.state # To get the Task state
    
    # Task state = Success 
    if task_state == 'success':
        exec_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M')
        slack_msg = f"""
        :white_check_mark: Success Run for [{context.get('task_instance').task_id}] at [{exec_date}].
        """
    # Task state = Failed 
    
    elif task_state =='failed':
        exec_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M')
        slack_msg = f"""
:alert: Airflow Task Failed <!here>
*Task*: {context.get('task_instance').task_id}
*Dag*: {context.get('task_instance').dag_id}
*Execution Date*: {exec_date}
*Error Log*: <{context.get('task_instance').log_url}|Link to Airflow Log>
        """

    slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_app')
    slack_hook.send(text=slack_msg)
```

**process_data.py**
```python
def process_data(nome_tabela, schema_origem, staging_bucket):
    try:
        
        import polars as pl
        import io
        import gzip
        from google.cloud import storage
        
        # path do arquivo que vai ser descompactado e processado
        input_file = 'output/output_{}_{}.csv.gz'.format(schema_origem, nome_tabela)
        
        # Setup GCS client
        client = storage.Client()
        bucket = client.get_bucket(staging_bucket)
        blob = bucket.blob(input_file)
        print('Leitura do Bucket finalizado com sucesso.')
        
        # Download dos bytes, pois o arquivo está gzipado
        compressed_data = blob.download_as_bytes()
        
        # Decompress and read the content
        with io.BytesIO(compressed_data) as bio:
            with gzip.GzipFile(fileobj=bio, mode='rb') as gz:
                data = gz.read().decode('utf-8')
        print('Descompactamento dos dados .gz finalizado com sucesso.')
    
        # processamento dos dados com polars, caso seja necessario alguma
        # transformacao, de tipos de colunas, realizar aqui
        
        dtypes = {
            # ...
        }
        df = pl.read_csv(io.StringIO(data), dtypes=dtypes)
        
        print('Leitura dos dados com Polars finalizado com sucesso.')
        
        # transformacao ou limpeza de dados deve ser adicionado abaixo

        df_clean = df
        # ...

        df_csv = df_clean.write_csv(datetime_format='%Y-%m-%d %H:%M:%S')

        # Define o blob para os dados processados
        output_blob = bucket.blob('output/processed_{}_{}.csv'.format(schema_origem, nome_tabela))
    
        # Salva os dados processados de volta no mesmo GCS bucket
        output_blob.upload_from_string(df_csv, content_type='text/csv')
        
        print('Dados processados salvos com sucesso.')
        
        client.close()
        del client
        
        print('Conexão com Client GCS fechada com sucesso')
        
    except Exception as e:
        print("Error in data processing:", str(e))
        raise
```

**get_column_info.py**
```python
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
        'timestamp with time zone': 'TIMESTAMP',
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
```

# Fluxo para adicionar uma nova tabela do PostgreSQL
---

1) **Criar o arquivo de tabela**

Em ```helpers.tabelas``` copie algum dos arquivos existentes (que nao seja uma tabela incremental), e edite as variáveis que estão indicadas que precisam de edição nos comentários do código.

2) **Edição de variáveis**

As variáveis que precisam de edição foram listadas acima na seção 'Arquivos de tabela', mas um extra é que, caso você queira automatizar a geração da variável ```colunas``` você pode usar usar a DDL da tabela e aplicar a função ```get_column_info``` (tudo isso já está explicado em comentários no código).

Caso não veja necessidade de automatizar, basta criar a ```colunas``` manualmente e comentar a parte do código que chama a função ```get_column_info```.

3) *(opcional)* **Extraíndo a DDL**

Abra o DBeaver >> Abra o banco de dados >> Abra a schema >> Dê dois cliques na tabela > No menu lateral esquerdo da tela que abriu encontre "DDL" >> Copie somente até a primeira ```;```, não precisa copiar junto nenhum statement de ```CREATE INDEX```

* OBS: garanta que todas as colunas da DDL estão envelopadas em "", pois a função usa REGEX para identificar o nome da coluna.

4) **Criar a DAG**

Copie uma DAG que seja de um banco PostgreSQL, e a única coisa que precisa editar é a primeira linha:

```python
from helpers.tabelas.<nome_pasta_fonte>.<nome_arquivo_tabela> import parametros_tabela
```

5) **Faça o upload de todos esses arquivos no Bucket do Airflow**

Suba todos esses arquivos nas pastas corretas dentro Bucket do Airflow, que pode ser acessado via UI:

Google Cloud Console > Composer > Selecione a instância > Abrir Pasta de DAGs (logo abaixo da barra de busca do Google Cloud Console)

6) **Teste o fluxo**

Por padrão, depois de adicionar uma nova DAG no bucket ela já vem pausada. Abra a Airflow Web UI, encontre a DAG e ative ela e acompanhe se o fluxo rodou com sucesso!

# Contribuidores
---

Arthur Lee - Data Analytics & Engineering Lead (last update: 13/01/2024)

...
