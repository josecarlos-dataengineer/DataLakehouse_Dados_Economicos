import sys 
sys.path.append(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\Analise_de_carteira\analise_carteira_env\Lib\site-packages')

from bs4 import BeautifulSoup
import datetime as dt
from datetime import datetime,date,time
import json
import logging
import pandas as pd
from pathlib import Path
import pprint
import pymongo
import pymysql
from sqlalchemy import create_engine
import os
from urllib.request import Request, urlopen
import mysql.connector



logging.basicConfig(
    
        level=logging.INFO,
        handlers=[logging.FileHandler("analise_carteira.log", mode='a'),
                  logging.StreamHandler()],
        format="%(message)s -  %(funcName)s - %(filename)s - %(asctime)s"
        )


class api_get():
    '''
    Esta classe busca dados do site fundamentus, e armazena em um pandas dataframe.
    Método: 
        fundamentus_df: 
          parametros
            - url
            - headers
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
    
        colunas_names = [col.getText() for col in soup.find('table', {'id': 'resultado'}).find('thead').findAll('th')]
        colunas = {i: col.getText() for i, col in enumerate(soup.find('table', {'id': 'resultado'}).find('thead').findAll('th'))}

        dados = pd.DataFrame(columns=colunas_names)
        dados['data_carga'] = dt.date.today()
        
        for i in range(len(soup.find('table', {'id': 'resultado'}).find('tbody').findAll('tr'))):
            linha = soup.find('table', {'id': 'resultado'}).find('tbody').findAll('tr')[i].getText().split('\n')[1:]
            inserir_linha = pd.DataFrame(linha).T.rename(columns=colunas)
            dados = pd.concat([dados, inserir_linha], ignore_index=True)
            
        return dados
    
    def data_cleaning(col:pd.Series()):  
        '''
        Função criada para tratar os numeros armazenados com texto em formato pt-br
        remove os sinais de %
        ajusta os sinais decimais
        '''  
        col = col.apply(lambda x: float(str(x).replace('.','').replace(',','.').replace('%','')))
        return col   


class env_builder():
    '''
    Classe desenvolvida para criação do ambiente, estrutura de pastas e
    conexão com o client.
    Métodos: \n
    * uri_scan() Verifica o contexto de execução \n
    * client_mongo() *Estabelace a conexão \n
    '''
    
    def uri_scan() -> str:
        '''
        Função utilizada para verificar se o script está sendo executado em 
        container ou localmente. Se for uma execução local, a função retorna
        a string "local", se não "container".    
        '''
        pwd = os.getcwd()
        if pwd == '/workspaces/app':
            logging.info(f"Ambiente:Container \n path:{pwd}")
            return 'container'
        else:
            logging.info(f"Ambiente:Local \n path:{pwd}")
            return 'local'
    def path_scan() -> str:
        '''
        Função utilizada para trazer automaticamente o caminho dos arquivos fonte".    
        '''
        pwd = str(os.getcwd()).replace("\\","/")+"/"
        
        return pwd
        
    def client_mongo():
        '''
        Esta função estabelece a conexão com o client mongo da biblioteca
        pymongo.
        Para selecionar a uri, é feita a chamada da função *uri_scan(), que 
        indica se o script está sendo executado em container ou localmente.
        '''
        
        uri_selector = {
            'local':'mongodb://localhost:27017/',
            'container':'mongodb://host.docker.internal:27017/',
            'container_SCRAM-SHA-1':'mongodb://user:example@host.docker.internal:27017/?authSource=the_database&authMechanism=SCRAM-SHA-1'
            }

        uri = uri_selector[env_builder.uri_scan()]
        try:
            myclient = pymongo.MongoClient(uri)
            logging.info(f"Client conectado! URI:{uri}")
        except Exception as e:
            logging.error(f"Conexão negada! URI:{uri}")
            pass
        
        return myclient
    def client_mysql():
        host = {
            'local':'localhost',
            'container':'host.docker.internal'            
            }
        host_str = host[env_builder.uri_scan()]
        
        try:
            mydb = mysql.connector.connect(
            host = host_str,
            user = 'root',
            password = 'root',
            database = 'db')

        except Exception as e:

            pass
        
        return mydb

class mongo_etl():
    '''
    Realiza a extração, transformação e carga de collections do mongodb
    através do uso dos métodos: \n
    transform: carrega uma lista de collections para uma lista de dicionarios.
    insert_into_mongo: carrega uma lista de arquivos json para o mongodb. \n
    
    '''
 
    # def insert_into_mongo(path:str,files:list,database:str):
    #     '''
    #     Esta função coleta os arquivos json (carteiras, operacoes e usuarios)
    #     e insere no mongodb.
    #     Deve-se passar os parâmetros:
    #     path: caminho onde estão armazenados os arquivos;
    #     files: lista dos nomes dos arquivos *com a extensão json;
    #     database: Nome do database que receberá as collections no Mongodb
        
    #     exemplo: \n
    #     insert_into_mongo( \n
    #         path='local/pasta/':str, \n
    #         files=['usuarios.json','carteiras.json','operacoes.json'], \n
    #         database='plataforma')       
        
    #     '''
        
    #     files = ['usuarios.json','carteiras.json','operacoes.json'] 

    #     myclient = env_builder.client_mongo()
    #     for f in files:
    #         logging.info(f"Início da carga do arquivo {f}")
    #         filename = f.strip('.json')
            
    #         with open(path+f, 'r+',encoding='UTF-8') as arquivo:
    #                 data = json.load(arquivo)   

    #         mydb = myclient[database]
            
    #         string_insert = f"mydb.{filename}.insert_many(data)"
    #         exec(string_insert)
    #         logging.info(f"fim da carga do arquivo {f}")
    #     logging.info(f"Os arquivos {str(files).strip('[]')} \n foram carregadas para o mongodb.")
            
            
    def mongo_to_dict_list(database:str,collection:str):
        '''
        Função que carrega documentos do mongodb para lista de dicionarios.
        database: Nome do database no mongodb;
        collection: Collection a ser extraída.    
        
        exemplo:
        mongo_to_dict_list(database='plataforma',collection='operacoes')\n
        Retornará uma lista de documentos da coleção operacoes.
        '''    
        myclient = env_builder.client_mongo()
        mydb = myclient[database]
        mycol = mydb[collection]
        doc_list = []
        n = 0
        for col in mycol.find({},{'_id':False}):
            
            doc_list.append(col)
            n += 1
        logging.info(f"{n} documentos da coleção {collection} foram carregados para a lista.")

        return doc_list


    def mongo_list_to_list_dict(database:str,collections:list):
        '''
        Função que retorna uma lista de dicionarios de lista de documentos.
        database: Nome do Database
        collections: lista de collections a buscar 
        
        exemplo:
        mongo_list_to_list_dict(
            database='plataforma',
            collections=['usuarios','carteiras','operacoes'])
            
        Retornará uma lista de dicionários de coleções contendo uma lista de documentos.
        '''
        dict_list = []
        param_dict = dict()
        n = 0
        for col in collections:
            param_dict = {col:mongo_etl.mongo_to_dict_list(database,col)}
            dict_list.append(param_dict)
            n += 1
        logging.info(f"{n} coleções do banco {database} foram carregadas para a\nlista de dicionarios..")

        return dict_list
    
    def carga_mongodb_one(path:str,database:str,file:str):
        '''
        Função que carrega arquivo json de ambiente local para mongodb.
        path: caminho onde estão salvos os arquivos.
        database: nome do banco que receberá os arquivos.
        files: Nome do arquivo com a extensão.
        '''
        
        
        myclient = env_builder.client_mongo()
        path = path
        database = database
        filename = file.replace('.json','')
            
        with open(path+file, 'r+',encoding='UTF-8') as arquivo:
                data = json.load(arquivo)   

                mydb = myclient[database]
                
                string_insert = f"mydb.{filename}.insert_many(data)"
                print(string_insert)
                exec(string_insert)
                logging.info(f"O arquivo {file} foram carregado para as coleção\n{str(file).strip('[]').replace('.json','')}\ndentro do database {database}")
        return None
    
    def carga_mongodb_many(path:str,database:str,files:list):
        '''
        Função que carrega uma lista de arquivos json de ambiente local para mongodb.
        path: caminho onde estão salvos os arquivos.
        database: nome do banco que receberá os arquivos.
        files: lista de arquivos a serem carregados com suas extensões.
        '''
        for file in files:
            logging.info(f"Carregando {file} para o database {database}")
            mongo_etl.carga_mongodb_one(path=path,database=database,file=file)
        return None
        
        
class mysql_etl():
    '''
    Classe que cria a conexão com mysql usando o método *criar. \n
    Parâmetros: \n
        host \n 
        user \n
        password \n
        database \n
    *host*:Para execução local do python usar host=localhost e host=host.docker.internal para execução em container.
    *user*:Usar o mesmo user mencionado no compose.yaml
    *password*:Usar o mesmo password mencionado no compose.yaml
    *database*:Usar o mesmo database mencionado no compose.yaml
    '''
    def __init__(self,host:str,user:str,password:str,database:str) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        
    def criar(self):    
        '''
        Método utiliza as bibliotecas sqlalchemy e pymysql para criar a conexão, utilizando os parametros passados como atributos da classe.        
        '''          
            
        # MySQL parametros da conexão com Mysql
        host = self.host
        user = self.user
        password = self.password
        database = self.database
        try:
            # Criação da conexão usando pymysql
            connection = pymysql.connect(host=host, user=user, password=password, database=database)

            # Criação da engine sqlalchemy
            engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
            logging.info(f"conexão criada para\nhost={self.host}\nuser={self.user}\npassword=****\ndatabase={self.database}")
        except Exception as e:
            logging.error("Revise os parametros passados para a conexão.")
            raise Exception('Acesso negado ao Mysql.')

        return engine

            