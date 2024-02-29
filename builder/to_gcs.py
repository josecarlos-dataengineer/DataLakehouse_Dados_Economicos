from conn_builder import aws_connection
from params import *
from dir_builder import builder, pipelines
import pandas as pd
import io

import os
from google.cloud import storage

key = builder(is_cloud=1,frequency='daily',schedule=1).key_builder()

os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r"C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\DataLakehouse_Dados_Economicos\builder\secrets\case-415614-bea26fb26936.json"

class s3_to_gcs():
        def run(context:str,filename:str,source:str,source_key:str,
            destination:str,destination_key:str):
                
                client = aws_connection(profile='admin',provider='s3').conn()
                # print(f"{source}/{source_key}/{filename}")
                data = client.get_object(Bucket=source,Key=f"{source_key}{filename}")
                df = pd.read_csv(data["Body"],sep=';',encoding='utf-8') 
                
                buffer = io.BytesIO()
                file = filename.replace('.csv','')
                storage_client =  storage.Client()
                bucket_name = 'dev-de-landing-case-415614'
                print(bucket_name)
                print(file)
                destination_path = f"dev-de-landing/{file}.parquet"
                
                bucket = storage_client.get_bucket(bucket_name)          

                bucket.blob(destination_path).upload_from_string(df.to_parquet(), 'parquet')
                
                # df.to_parquet(buffer,engine='pyarrow',compression='snappy')
                # filename = filename.replace('.csv','.parquet')
                # client.put_object(ACL='private',
                #         Body=buffer.getvalue(),
                #         Bucket=destination,
                #         Key=f"{destination_key}{filename}")
                
                return None  
            
filelist = ['dfp_cia_aberta_DRE_con_2023.csv','dfp_cia_aberta_DRE_ind_2023.csv','cad.csv','fundamentus.csv','fundamentei.csv']

context = 'case'
filename = 'dfp_cia_aberta_DRE_con_2023.csv'
source = 'dev-de-processed-727477891012'
source_key = f'{key}/{context}/'
destination = 'dev-de-bronze-727477891012'
destination_key = f'{key}/{context}/'
for filename in filelist:
    s3_to_gcs.run(context = context,
    filename = filename,
    source = source,
    source_key = source_key,
    destination = destination,
    destination_key = destination_key)