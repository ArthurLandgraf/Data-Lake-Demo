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