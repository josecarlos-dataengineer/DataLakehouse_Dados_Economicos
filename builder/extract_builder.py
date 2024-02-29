
# ----------------------------------------------------------------------------
# ---------------------- Importação das bibliotecas --------------------------
import boto3
from bs4 import BeautifulSoup
from conn_builder import aws_connection
import datetime as dt
import io
import json
import logging
import os
import pandas as pd
from pathlib import Path
import re
import requests
from urllib.request import Request, urlopen
from tempfile import TemporaryDirectory
import zipfile
from params import *
from dir_builder import builder, pipelines

key = builder(is_cloud=1,frequency='daily',schedule=1).key_builder()



# ----------------------------------------------------------------------------
# ---------------------- Definição do logging --------------------------------
logging.basicConfig(
    
        level=logging.INFO,
        handlers=[logging.FileHandler("extract.log", mode='w'),
                  logging.StreamHandler()],
        format="%(message)s -  %(funcName)s - %(filename)s - %(asctime)s")

# ----------------------------------------------------------------------------
# ---------------------- Extração da tabela fundamentus ----------------------

class api_get_fundamentus():
    '''
    Esta classe busca dados do site fundamentus, e armazena em um pandas 
    dataframe.
    Método: 
        fundamentus_df: 
          parametros
            - url
            - headers \n
    exemplo de chamada: \n
    # api_get_fundamentus(url='https://www.fundamentus.com.br/resultado.php',
#         headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) 
# AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}).fundamentus_df()
        '''        
    def __init__(self,url:str,headers:dict):
        self.url = url
        self.headers = headers
        
    def fundamentus_df(self):
        
        '''
        parametros
            - url
            - headers               
        '''
        req = Request(self.url, headers = self.headers)
        response = urlopen(req)
        html = response.read()
        soup = BeautifulSoup(html, 'html.parser')    
    
        colunas_names = [col.getText() for col in soup.find('table', \
            {'id': 'resultado'}).find('thead').findAll('th')]
        colunas = {i: col.getText() for i, col in enumerate(soup.find('table',\
            {'id': 'resultado'}).find('thead').findAll('th'))}

        dados = pd.DataFrame(columns=colunas_names)
        dados['data_carga'] = dt.date.today()
        
        iterate = range(len(soup.find('table', {'id': 'resultado'})\
            .find('tbody').findAll('tr')))
        
        for i in iterate:
            linha = soup.find('table', {'id': 'resultado'}).find('tbody')\
                .findAll('tr')[i].getText().split('\n')[1:]
                
            inserir_linha = pd.DataFrame(linha).T.rename(columns=colunas)
            dados = pd.concat([dados, inserir_linha], ignore_index=True)
            
        return dados
    
    def data_cleaning(col:pd.Series()):  
        '''
        Função criada para tratar os numeros armazenados com texto em formato
        pt-br
        remove os sinais de %
        ajusta os sinais decimais
        '''  
        col = col.apply(lambda x: float(str(x).replace('.','').replace(',','.')\
            .replace('%','')))
        return col  

# ----------------------------------------------------------------------------
# ---------------------- Extração das DFPs -----------------------------------
class api_get_DFPS:  
    '''
    tipo: Classe
    objetivo: Coletar os arquivos DFPs do portal 
    'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/' \n
    Métodos:\n
    get_file_list(): Retorna o último elemento da lista dos arquivos .zip
    da url. Este último arquivo é o mais atual.\n
    parse_url(): Retorna o conteúdo do resultado da *get_file_list()\n
    api_resp_to_buffer: Retorna o resultado da *parse_url() como arquivo em 
    buffer 
    
    
    ''' 
       
    def get_file_list(): 
        
        url = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, 'html.parser') 
         
        url_list = []
        files_text = soup.get_text()
        file_list = re.findall(r'\b\w+\.zip\b',files_text)    
      
        for i in file_list:
            ul = f'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/{i}'
            url_list.append(ul)         
            
        return url_list[-1]

    def parse_url():
        
        url = api_get_DFPS.get_file_list()
        response = requests.get(url) 
        resp = response.content
        bytesbuffer = io.BytesIO()
        bytesbuffer.write(response.content)
        zipped = zipfile.ZipFile(bytesbuffer)
        file_list = zipped.namelist()
    
        return resp

    # def api_resp_to_s3(api_resp,target_bucket,key,provider,profile):
    def api_resp_to_buffer(api_resp):
        '''
        Exemplo de chamada:
        # buffer = api_get_DFPS.api_resp_to_buffer(api_get_DFPS.parse_url()) 

        '''  
   
        with TemporaryDirectory() as temp_dir:
            
            # setting up the temp paths which are used for store temp files
            data = Path(temp_dir) / 'source.zip'
            root = Path(temp_dir) 
            z_files = Path(temp_dir) / 'z_files/'
            
            
            with open(data,'wb') as file:
                file.write(api_resp)
            return api_resp
    # def extract_zip_from_s3():
            #     with zipfile.ZipFile(data) as zipped:
            #         zipped.extractall(z_files)
                                
            #         for file in os.listdir(z_files):                
            #             df = pd.read_csv(f"{z_files}/{file}",sep=';',encoding='cp1252')

            #             buffer = io.StringIO()
            #             df.to_csv(buffer,sep=';',encoding='utf-8',index=None)
            # return buffer 


class api_get_cad():   
        
    def url_to_buffer():
        '''
        # x = api_get_cad(
        #url_part='https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/',
        #filenameURL='cad_cia_aberta.csv').url_to_buffer()        
        '''
        
        url_part='https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/'
        filenameURL='cad_cia_aberta.csv'
        
        url = f"{url_part}{filenameURL}"

        df = pd.read_csv(url,sep=';',encoding='Windows-1252')
        buffer = io.StringIO()    
        df.to_csv(buffer,encoding='utf-8',sep=';',index=None)
        
        return buffer    
    
class write_to_s3():
      
    def dfp_to_s3(target_bucket,key,file):
        client = aws_connection(profile='admin',provider='s3').conn()
        buffer = api_get_DFPS.api_resp_to_buffer(api_get_DFPS.parse_url())      
        
        client.put_object(
                        Body=buffer,
                        Bucket=target_bucket,
                        Key=f'{key}{file}')
        
    def cad_to_s3(target_bucket,key,file):
        client = aws_connection(profile='admin',provider='s3').conn()
        buffer = api_get_cad.url_to_buffer()      
        
        client.put_object(
                        Body=buffer.getvalue(),
                        Bucket=target_bucket,
                        Key=f'{key}{file}')
        
    def fundamentus_to_s3(target_bucket,key,file):
        client = aws_connection(profile='admin',provider='s3').conn()
        df = api_get_fundamentus(url='https://www.fundamentus.com.br/resultado.php',
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 \
                Safari/537.36'}).fundamentus_df()
        
        buffer = io.StringIO()
        df.to_csv(buffer,sep=';',encoding='utf-8',index=None)
        
        client.put_object(
                        Body=buffer.getvalue(),
                        Bucket=target_bucket,
                        Key=f'{key}{file}')
        
        return None
        
class fundamentei_files():
    def to_s3(destination:str,key:str,context:str):
        client = aws_connection(profile='admin',provider='s3').conn()
        lista_de_arquivos = os.listdir('C:/Users/SALA443/Desktop/Projetos/josecarlos-dataengineer/template/criação de ambiente/builder/sources')

        for arquivo in lista_de_arquivos:
            if arquivo[-4:] == '.csv':
                lista_de_arquivos.remove(arquivo)
                for i in lista_de_arquivos:    
                    with open(f"C:/Users/SALA443/Desktop/Projetos/josecarlos-dataengineer/template/criação de ambiente/builder/sources/{i}", 'r') as arquivo:        
                        data = json.load(arquivo)              
                        data = json.dumps(data,indent=2)
                        client.put_object(
                                        Body=data,
                                        Bucket=destination,
                                        Key=f'{key}/{context}/{i}')
                        
                        
    def run(source:str,destination:str,key:str,context:str,lista_de_arquivos:list):
        client = aws_connection(profile='admin',provider='s3').conn()
        dataframe = pd.read_json(f's3://{source}/{key}/{context}/document0.json')
        dataframe = dataframe[:0]
        
         
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
        
        

       
       
       





