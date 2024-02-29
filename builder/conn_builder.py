
# ----------------------------pep8 line lenght -------------------------------
import sys
sys.path.append(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\DataLakehouse_Dados_Economicos\builder_venv\Lib\site-packages')
import boto3
from dotenv import load_dotenv
from dir_builder import builder,pipelines
import logging
import mysql.connector
from pathlib import Path
import pymongo
import os

# ----------------------------------------------------------------------------
# ---------------------- Definição de logging --------------------------------

logging.basicConfig(
    
        level=logging.INFO,
        handlers=[logging.FileHandler("directory.log", mode='w'),
                  logging.StreamHandler()],
        format="%(message)s -  %(funcName)s - %(filename)s - %(asctime)s")

# ----------------------------------------------------------------------------
# ---------------------- Definição de variáveis ------------------------------

dotenv_path = Path(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\template\criação de ambiente\builder\secrets\.env')
load_dotenv(dotenv_path=dotenv_path)

# ----------------------------------------------------------------------------
# ---------------------- Definição das conexões (Databases)-------------------

class database_connections():
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
        'container_SCRAM-SHA-1':'mongodb://user:example@host.docker.internal \
:27017/?authSource=the_database&authMechanism=SCRAM-SHA-1'
            }

        uri = uri_selector[pipelines.path_scan()]
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
        host_str = host[pipelines.path_scan()]
        
        try:
            mydb = mysql.connector.connect(
            host = host_str,
            user = 'root',
            password = 'root',
            database = 'db')

        except Exception as e:

            pass
        
        return mydb
# ----------------------------------------------------------------------------
# ---------------------- Definição das conexões (CLOUD) ----------------------
class aws_connection():
    '''
    This class performs the aws connection, by fetching 
    credentials within .env file.
    
    account method: Fetches access_key_id and access_secret_id in .env and 
    returns a dictionary containning sensitive information.
    
    conn method: Establish the connection and returns client object.
    
    aws_connection('admin','s3').conn()
    '''
    def __init__(self,profile:str,provider:str,aws_access_key=None,
                 aws_access_secret_key=None):
        
        self.profile    = profile   
        self.provider   = provider   
          
        pipeline_method = pipelines.on_off(1)  
        
        log_dict = {
            1:"The pipeline method was set as automatic.",
            0:"The pipeline method was set as manual, which means some of\
parameters will be required manually"}   
        
        # log checkpoint
        logging.info(log_dict[pipeline_method])   
        
        if pipeline_method == 0: 
            self.aws_access_key = input("Please enter your access key id!") 
            self.aws_access_secret_key = input("Please type your secret key!") 
        else:
            self.aws_access_key = aws_access_key
            self.aws_access_secret_key = aws_access_secret_key 
  
    @property
    def account(self) -> dict:
        
        if self.profile == "admin":
            
            # log checkpoint
            logging.info('admin profile was chosen')
            return {"aws_access_key_id":os.getenv("aws_access_key_id"),
                    "aws_secret_access_key":os.getenv("aws_secret_access_key"),
                    "aws_region":os.getenv("aws_region")}
            
        else:
            # log checkpoint
            logging.info("Invalid profile or .env file secrets!")
            
            return {
        "aws_access_key_id":os.getenv("excepetion_aws_access_key_id"),
        "aws_secret_access_key":os.getenv("excepetion_aws_secret_access_key"),
        "aws_region":os.getenv("excepetion_aws_region")}
    
    def conn(self):
        
        try:            
            credentials = aws_connection(self.profile,self.provider).account            
            client = boto3.client(self.provider,                              
                aws_access_key_id     =   credentials['aws_access_key_id'],
                aws_secret_access_key =   credentials['aws_secret_access_key']
                )
            logging.info(f"Access granted for credentials starting with \
{credentials['aws_access_key_id'][:7]}")
            
            return client           
        
        except ValueError(f"Access denied! Please check if the \
aws_access_key_id and aws_secret_access_key are correctly typed in .env \
for {self.profile} profile."):
            logging.error(f"Access denied for {self.profile}")
            
    def conn_session(self):
        
        try:
            credentials = aws_connection(self.profile,self.provider).account
            session = boto3.Session(
                aws_access_key_id= credentials['aws_access_key_id'],
                aws_secret_access_key= credentials['aws_secret_access_key'],
            )
            logging.info(f"Access granted for credentials starting with \
{credentials['aws_access_key_id'][:7]}")
        except:
            logging.error(f"Access denied for {self.profile}")
        
        return session
    
    
    def listing_files_from_a_bucket(bucket,prefix):
            '''
            Function developed for iterate over a given s3 bucket/key and stores
            each file name into a list.
            
            Returns a list.
            
            By passing bucket name and prefix, it is possible to retrieve the file names.
            
            I.E
            listing_files_from_a_bucket(
                'de-okkus-landing-dev-727477891012',
                '2023/12/31/fundamentus')
            '''
            
            client = aws_connection(profile='admin',provider='s3').conn()

            s3_response = client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
            
            s3_file_list = []
            for x in s3_response['Contents']: 
                x = str(x['Key']).split('/')
                s3_file_list.append(x[-1])   

            return s3_file_list
            

