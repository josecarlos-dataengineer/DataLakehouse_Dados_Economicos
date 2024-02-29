# ----------------------------pep8 line lenght -------------------------------
import sys
sys.path.append(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\DataLakehouse_Dados_Economicos\builder_venv\Lib\site-packages')
import requests



from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import json
import pandas as pd

# Path to GeckoDriver executable
gecko_driver_path = r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\template\criação de ambiente\builder\secrets\geckodriver.exe'

# Set the path to the GeckoDriver executable as an environment variable
import os
os.environ['webdriver.gecko.driver'] = gecko_driver_path

# Initialize a WebDriver instance for Firefox
driver = webdriver.Firefox()

# tickers = ['petr3']
# for ticker in tickers:
# url = f"https://fundamentei.com/br/{petr3}"

lista = pd.read_csv(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\template\criação de ambiente\builder\sources\acoes-listadas-b3.csv',sep=',',encoding='utf-8')
tickers1 = list(lista['Ticker'])[0:50]
tickers2 = list(lista['Ticker'])[51:100]
tickers3 = list(lista['Ticker'])[101:150]
tickers4 = list(lista['Ticker'])[151:200]
tickers5 = list(lista['Ticker'])[201:250]
tickers6 = list(lista['Ticker'])[251:300]
tickers7 = list(lista['Ticker'])[301:350]
tickers8 = list(lista['Ticker'])[351:400]
tickers9 = list(lista['Ticker'])[401:450]
tickers10 = list(lista['Ticker'])[451:500]
tickers11 = list(lista['Ticker'])[501:535]

tickers = [tickers1,tickers2,tickers3,tickers4,tickers5,tickers6,tickers7,tickers8,tickers9,tickers10,tickers11]
for n in range(0,11):
    for ticker in tickers[n]:
        x = requests.get('https://fundamentei.com/br/show3')
        if x.status_code == 200:
            try:
                driver.get(f'https://fundamentei.com/br/{ticker}')
            except:
                continue
            try:
                cnpj = driver.find_element(By.XPATH,'/html/body/div[1]/div[2]/div[2]/div[1]/div[2]/div[2]/div[1]/div[2]/div[1]/div')
            except:
                cnpj = 'na'
                
            try:
                tickeron = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[1]/div/div[1]/div[2]/div/a[1]')
            except:
                tickeron = 'na'
            try:
                tickerpn = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[1]/div/div[1]/div[2]/div/a[2]')
            except:
                tickerpn = 'na'
                
            try:
                nome = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[1]/div[1]/div/div[1]/div[1]/h1")
            except:
                nome = 'na'
            try:    
                atuacao = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[1]/div/div[2]')
            except:
                atuacao = 'na'
                
            try:
                segmento_listagem = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]/div/div[2]/div[1]/span')
            except:
                segmento_listagem = 'na'
            try:
                
                tag_alongon = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]/div/div[2]/div[2]/div/div[1]')
            except:
                tag_alongon = 'na'
            try:
                tag_alongpn = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]/div/div[2]/div[2]/div/div[2]')
            except:
                tag_alongpn = 'na'
            try:
                freefloaton = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]/div/div[2]/div[3]/div/div[1]')
            except:
                freefloaton = 'na'
            try:
                freefloatpn = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]/div/div[2]/div[3]/div/div[2]')
            except:
                freefloatpn = 'na'
            try:
                on_percent = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]/div/div[3]/div[1]/ul/li[1]/div[2]/span[1]')
            except:
                on_percent = 'na'
            try:
                pn_percent = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]/div/div[3]/div[1]/ul/li[1]/div[2]/div')
            except:
                pn_percent = 'na'
                
            try:
                socio = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]/div/div[3]/div[1]/ul/li[1]/div[2]/span[2]')
            except:
                socio = 'na'
            try:
                reclameaquinota = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div/span[1]')
            except:
                reclameaquinota = 'na'
            try:
                
                reclameaquitexto = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div/span[2]')
            except:
                reclameaquitexto = 'na'
            try:
                
                reclameaquiatualizadoem = driver.find_element(By.XPATH,'/html/body/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[2]/div/div[2]/div[2]/time')
            except:
                reclameaquiatualizadoem = 'na'

            try:
                cnpj = cnpj.text
            except:
                pass
            try:    
                tickeron = tickeron.text
            except:
                pass
            try:
                tickerpn = tickerpn.text
            except:
                pass
            try:
                nome = nome.text.replace('•','-')
            except:
                pass
            try:
                atuacao = atuacao.text 
            except:
                pass
            try:
                segmento_listagem = segmento_listagem.text
            except:
                pass
            try:
                tag_alongon = tag_alongon.text.replace(' ',' - ')
            except:
                pass
            try:
                tag_alongpn = tag_alongpn.text.replace(' ',' - ')
            except:
                pass    
            try:
                freefloaton = freefloaton.text.replace(' ',' - ')
            except:
                pass
            try:    
                freefloatpn = freefloatpn.text.replace(' ',' - ')
            except:
                pass    
            try:
                on_percent = on_percent.text.replace(' ',' - ')
            except:
                pass   
            try:
                pn_percent = pn_percent.text.replace(' ',' - ')
            except:
                pass    
            try:
                socio = socio.text
            except:
                pass 
            try:   
                reclameaquinota = reclameaquinota.text
            except:
                pass
            try:
                reclameaquitexto = reclameaquitexto.text
            except:
                pass
            try:
                reclameaquiatualizadoem = reclameaquiatualizadoem.text
            except:
                pass

            document = {
            'cnpj' : cnpj,
            'tickeron' : tickeron,
            'tickerpn' : tickerpn,
            'nome' : nome,
            'atuacao' : atuacao,
            'segmento_listagem' : segmento_listagem,
            'tag_alongon' : tag_alongon,
            'tag_alongpn' : tag_alongpn,
            'freefloaton' : freefloaton,
            'freefloatpn' : freefloatpn,
            'on_percent' : on_percent,
            'pn_percent' : pn_percent,
            'socio' : socio,
            'reclameaquinota' : reclameaquinota,
            'reclameaquitexto' : reclameaquitexto,
            'reclameaquiatualizadoem' : reclameaquiatualizadoem
            }

            # list_doc.append(document)
            # file = json.dumps(document,indent=2)
            if os.path.isfile(f'document{n}.json') == False:
                
                with open(f'document{n}.json', 'w', encoding='utf-8') as arquivo:
                            arquivo.write("[]")
                            
                            
                with open(f"document{n}.json", 'r') as arquivo:
                        data = json.load(arquivo)            
                        data.append(document)
                        
                with open(f"document{n}.json", mode='w', encoding='utf-8') as arquivo:
                        json.dump(data, arquivo,indent=4)
            else:
                with open(f"document{n}.json", 'r') as arquivo:
                        data = json.load(arquivo)            
                        data.append(document)
                        
                with open(f"document{n}.json", mode='w', encoding='utf-8') as arquivo:
                        json.dump(data, arquivo,indent=4)
                

