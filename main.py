import os
import sys
from pathlib import Path

# pwd = '/workspaces/app/'

pwd = 'C:/Users/SALA443/Desktop/Projetos/josecarlos-dataengineer/DataLakehouse_Dados_Economicos/'
sys.path.append(os.path.dirname(pwd))
sys.path.append(fr'{pwd}/builder')

# sys.path.append(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\template\criação de ambiente\builder')

from builder import params,builder, write_to_s3,landing_to_processed,processed_to_bronze,fundamentei_files,aws_connection

key = builder(is_cloud=1,frequency='daily',schedule=1).key_builder()

# Carregar para S3 landing
    
layer, bucket, path, key = builder(
                                    is_cloud=1,
                                    frequency='daily',
                                    schedule=1           
                                    ).mount(
                                        prefix='de',
                                        provider='s3',
                                        layer='1',
                                        arquitecture='datalake',
                                        root='de',
                                        context='case')

write_to_s3.cad_to_s3(target_bucket=bucket,key=key,file='cad.csv')

write_to_s3.dfp_to_s3(target_bucket=bucket,key=key,file='dfp.zip')

write_to_s3.fundamentus_to_s3(target_bucket=bucket,key=key,file='fundamentus.csv')


# Carregar para S3 processed
# chamada dfp
context = 'case'
filenames = ['dfp_cia_aberta_DRE_con_2023.csv','dfp_cia_aberta_DRE_ind_2023.csv']
source = 'dev-de-landing-727477891012'
source_key = f'{key}dfp.zip'
destination = 'dev-de-processed-727477891012'
destination_key = f'{key}'

for filename in filenames:
    landing_to_processed.dfp(context = context,
    filename = filename,
    source = source,
    source_key = source_key,
    destination = destination,
    destination_key = destination_key)

# chamada cad
context = 'case'
filename = 'cad.csv'
source = 'dev-de-landing-727477891012'
source_key = f'{key}cad.csv'
destination = 'dev-de-processed-727477891012'
destination_key = f'{key}'

df = landing_to_processed.cad(context = context,
filename = filename,
source = source,
source_key = source_key,
destination = destination,
destination_key = destination_key)

# chamada fundamentus
context = 'case'
filename = 'fundamentus.csv'
source = 'dev-de-landing-727477891012'
source_key = f'{key}fundamentus.csv'
destination = 'dev-de-processed-727477891012'
destination_key = f'{key}'

df = landing_to_processed.fundamentus(context = context,
filename = filename,
source = source,
source_key = source_key,
destination = destination,
destination_key = destination_key)

# # chamada da fundamentei_files.to_s3      
# destination = f"{params.enviroments['dev']}-{params.prefixes}-{params.layers['datalake']['1']}-{params.sufixes['s3']}"
# key
# context = 'case' 
# fundamentei_files.to_s3(destination=destination,key=key,context=context)
# # chamada da fundamentei to processed    
# source = f"{params.enviroments['dev']}-{params.prefixes}-{params.layers['datalake']['1']}-{params.sufixes['s3']}"  
# destination = f"{params.enviroments['dev']}-{params.prefixes}-{params.layers['datalake']['2']}-{params.sufixes['s3']}"  
# context = 'case'  
# # listadearquivos =  aws_connection.listing_files_from_a_bucket(bucket=params.layers['datalake']['1'],prefix=key+'/'+context)
# listadearquivos = ['document0.json','document1.json','document2.json','document3.json','document4.json','document5.json','document6.json','document7.json','document8.json','document9.json','document10.json']
# fundamentei_files.run(source=source,destination=destination,key=key,context=context,lista_de_arquivos=listadearquivos)       

# chamada parquet
filelist = ['dfp_cia_aberta_DRE_con_2023.csv','dfp_cia_aberta_DRE_ind_2023.csv','cad.csv','fundamentus.csv']

context = 'case'
filename = 'dfp_cia_aberta_DRE_con_2023.csv'
source = 'dev-de-processed-727477891012'
source_key = f'{key}'
destination = 'dev-de-bronze-727477891012'
destination_key = f'{key}'
for filename in filelist:
    processed_to_bronze.run(context = context,
    filename = filename,
    source = source,
    source_key = source_key,
    destination = destination,
    destination_key = destination_key)