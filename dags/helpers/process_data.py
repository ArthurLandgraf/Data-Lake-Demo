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
