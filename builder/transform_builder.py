import boto3
from conn_builder import aws_connection
import pandas as pd
import zipfile
import io
from io import BytesIO
from metadata_builder import create_metadata
from dir_builder import builder
import os

key = builder(is_cloud=1,frequency='daily',schedule=1).key_builder()

class landing_to_processed():
    '''
    Classe desevolvida para executar a transferência de dados entre os buckets
    Métodos: \n
    dfp: Faz descompactação dos arquivos DRE e insere campos metadados.
    '''
    
    def dfp(context:str,filename:str,source:str,source_key:str,
            destination:str,destination_key:str):  
        '''
        
        '''  
        session = aws_connection(profile='admin',provider='s3').conn_session()
        s3_resource = session.resource('s3')
        # s3_resource = boto3.resource('s3')
        zip_obj = s3_resource.Object(bucket_name=source, key=source_key)
 
        buffer = BytesIO(zip_obj.get()["Body"].read())

        z = zipfile.ZipFile(buffer)
        
        file_info = z.getinfo(filename)
        with z.open(filename) as f:
            df = pd.read_csv(f,sep=';',encoding='Windows-1252')
            create_metadata(df)
            buffer = io.StringIO()
            df.to_csv(buffer,sep=';',encoding='utf-8',index=None)
            client = session.client('s3')
            client.put_object(ACL='private',
            Body=buffer.getvalue(),
            Bucket=destination,
            Key=f"{destination_key}{filename}")
                
        return None
                
    def cad(context:str,filename:str,source:str,source_key:str,
            destination:str,destination_key:str):
        
        client = aws_connection(profile='admin',provider='s3').conn()

        data = client.get_object(Bucket=source,Key=f"{source_key}")
        df = pd.read_csv(data["Body"],sep=';',encoding='utf-8') 
        create_metadata(df)
        
        buffer = io.StringIO()
        df.to_csv(buffer,sep=';',encoding='utf-8',index=None)
        
        client.put_object(ACL='private',
                Body=buffer.getvalue(),
                Bucket=destination,
                Key=f"{destination_key}{filename}")
        
        return None
    
    def fundamentus(context:str,filename:str,source:str,source_key:str,
            destination:str,destination_key:str):
        
        client = aws_connection(profile='admin',provider='s3').conn()

        data = client.get_object(Bucket=source,Key=f"{source_key}")
        df = pd.read_csv(data["Body"],sep=';',encoding='utf-8') 
        create_metadata(df)
        
        buffer = io.StringIO()
        df.to_csv(buffer,sep=';',encoding='utf-8',index=None)
        
        client.put_object(ACL='private',
                Body=buffer.getvalue(),
                Bucket=destination,
                Key=f"{destination_key}{filename}")
        
        return None

    def run(source:str,destination:str,key:str,context:str):
        client = aws_connection(profile='admin',provider='s3').conn()
        dataframe = pd.read_json(f'sources/document0.json')
        dataframe = dataframe[:0]
        
        lista_de_arquivos = os.listdir('sources')

        for arquivo in lista_de_arquivos:
            if arquivo[-4:] == '.csv':
                lista_de_arquivos.remove(arquivo)

        for arquivo in lista_de_arquivos:
            # print(f's3://{source}/{key}/{context}/{arquivo}')
            data = pd.read_json(f's3://{source}/{key}/{context}/{arquivo}')
            frames = [dataframe,data]
            dataframe = pd.concat(frames)

        buffer = io.StringIO()
        dataframe.to_csv(buffer,sep=';',encoding='utf-8',index=None)
        
        client.put_object(
                        Body=buffer.getvalue(),
                        Bucket=destination,
                        Key=f'{key}/{context}/fundamentei.csv')


class processed_to_bronze():
        def run(context:str,filename:str,source:str,source_key:str,
            destination:str,destination_key:str):
                
                client = aws_connection(profile='admin',provider='s3').conn()
                # print(f"{source}/{source_key}/{filename}")
                data = client.get_object(Bucket=source,Key=f"{source_key}{filename}")
                df = pd.read_csv(data["Body"],sep=';',encoding='utf-8') 
                create_metadata(df)
                
                buffer = io.BytesIO()
                df.to_parquet(buffer,engine='pyarrow',compression='snappy')
                filename = filename.replace('.csv','.parquet')
                client.put_object(ACL='private',
                        Body=buffer.getvalue(),
                        Bucket=destination,
                        Key=f"{destination_key}{filename}")
                
                return None         

