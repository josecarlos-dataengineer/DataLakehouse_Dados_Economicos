
import pandas as pd
import os
import json
import io

# dataframe = pd.read_json(f'sources/document0.json')
# dataframe = dataframe[:0]

# lista_de_arquivos = os.listdir('sources')

# for arquivo in lista_de_arquivos:
#     if arquivo[-4:] == '.csv':
#         lista_de_arquivos.remove(arquivo)

# for arquivo in lista_de_arquivos:
    
#     data = pd.read_json(f'sources/{arquivo}')
#     frames = [dataframe,data]
#     dataframe = pd.concat(frames)

lista_de_arquivos =  os.listdir('sources')

for arquivo in lista_de_arquivos:
    if arquivo[-4:] == '.csv':
        lista_de_arquivos.remove(arquivo)
  
for i in lista_de_arquivos:    
    with open(f"sources/{i}", 'r') as arquivo:        
        data = json.load(arquivo)    
