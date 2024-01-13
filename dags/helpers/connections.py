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
