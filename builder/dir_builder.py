# ----------------------------------------------------------------------------
# ---------------------- Importação dos módulos ------------------------------
import datetime as dt
from datetime import datetime,date,time
import logging
import os
import sys
from data_quality import qualitiy_check
from params import *


# ----------------------------------------------------------------------------
# ---------------------- Definição de logging --------------------------------

logging.basicConfig(
    
        level=logging.INFO,
        handlers=[logging.FileHandler("directory.log", mode='w'),
                  logging.StreamHandler()],
        format="%(message)s -  %(funcName)s - %(filename)s - %(asctime)s")
      
# ---------------------------------------------------------------------------- 
# ---------------------- Gerenciamento de pipeline ---------------------------
class pipelines():
    
    def path_scan() -> str:
        
        '''
        Função utilizada para verificar se o script está sendo executado em 
        container ou localmente. Se for uma execução local, a função retorna
        a string "local", se não "container".    
        '''
        pwd = os.getcwd()
        if pwd == '/workspaces/app':
            return 'container'
        else:
            return 'local'
     
    def on_off(schedule):  
        
        if schedule == 0:
            logging.info('This pipeline is running as "manual"')
            return 0
        elif schedule == 1:
            logging.info('This pipeline is running as "scheduled"')
            return 1 
        else:
            error_msg = '''Please insert a valid number! (1 for scheduled  
pipelines or 0 for manually triggered pipelines)'''

            logging.error(error_msg) 
            

           
# ---------------------------------------------------------------------------- 
# ---------------------- Criação de diretórios -------------------------------
class builder():
    '''
    Classe desenvolvida para criação do ambiente, estrutura de pastas e
    conexão com o client.
    Métodos: \n
    * key_builder() retorna a estrutura da key folder \n
    * mount() retorna o caminho do diretório \n
    Se o parametro is_cloud = 0 {local}:
    retorna mnt, path, root, key
    Se o parametro is_cloud = 1 {Cloud}:
    retorna mnt, path, key
    '''
    def __init__(self,is_cloud:bool,frequency:str,schedule:int):
        
        self.is_cloud = is_cloud
        self.frequency = frequency
        self.schedule = schedule
           
    def key_builder(self):
        
        year = dt.date.today().year
        month = dt.date.today().month
        month = str(month).zfill(2)
        day = dt.date.today().day
        day = str(day).zfill(2)
        hour = datetime.now().strftime("%H")
        hour = str(hour).zfill(2)
                
        if  self.frequency == 'daily':
            return f"{year}/{month}/{day}"
        
        elif self.frequency == 'monthly':
            return f"{year}/{month}"
        
        elif self.frequency == 'hourly': 
            return f"{year}/{month}/{day}/{hour}"
        
        else:
# --------------------------pep8 79 lenght line-------------------------------        
            logging.error("Enter a valid frequency! [daily, monthly, hourly] \
is allowed.")
            
            raise TypeError("A invalid value was typed for frequency. Please \
check if it was mistyped or it is a new frequency that has to be included \
in valid frequency list")   
      
    def mount(self,provider=None,layer=None,arquitecture=None,prefix=None,
              root=None,context=None,sufix=None):
        
        '''
        Função utilizada para trazer automaticamente o caminho dos arquivos:
        exemplos de chamada: \n
        
        #local (is_cloud = 0)
        
        mnt, path, root, key = builder(
        is_cloud=0,
        frequency='daily',
        schedule=1            
        ).mount(root='de',
        context='case')
        
        #cloud (is_cloud = 1)
        
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
        '''
        qualitiy_check.params_check(
        is_cloud=self.is_cloud,
        frequency=self.frequency,
        schedule=self.schedule,
        provider=provider,
        prefix=prefix,
        sufix=provider,
        layer=layer,
        arquitecture=arquitecture)
        
        key = builder(self.is_cloud,self.frequency,self.schedule).key_builder()
        
        if self.is_cloud == 0:
            
            str_mnt = f"{str(os.getcwd())}/{root}/{key}/{context}"
            mnt = str_mnt.replace('\\','/')+'/'
            sys.path.append(os.path.dirname(mnt))
            
            path = f"{str(os.getcwd())}/".replace('\\','/')
            root = f"{root}/"
            key = f"{key}/{context}"
            
            return mnt, path, root, key
        
        else:
            
            layer = layers[arquitecture][layer]
            sufix = sufixes[provider]
            
            bucket = f"dev-{prefix}-{layer}-{sufix}"
            path = f"{bucket}/{key}/{context}"                     
            key = f"{key}/{context}/"
                    
            return layer, bucket, path, key
        
    
