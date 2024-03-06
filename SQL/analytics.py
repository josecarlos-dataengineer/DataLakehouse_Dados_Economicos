# Append do caminho do venv python ***desnecessário quando executado em container***
# import sys 
# sys.path.append(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\Analise_de_carteira\analise_carteira_env\Lib\site-packages')

# importação dos módulos
from environment import mysql_etl
import pandas as pd
import re


# fundamentus = pd.read_parquet('fundamentus.parquet')
# # tratamento de dados para ajustar os nomes das colunas e os tipos de dados
# columns_map = {"Papel":"papel",
# "Cotação":"cotacao",
# "P/L":"pl",
# "P/VP":"pvp",
# "PSR":"psr",
# "Div.Yield":"div_yield",
# "P/Ativo":"p_ativo",
# "P/Cap.Giro":"p_cap_giro",
# "P/EBIT":"p_ebit",
# "P/Ativ Circ.Liq":"p_ativ_circ_liq",
# "P/EBIT":"p_ebit",
# "P/Ativ Circ.Liq":"p_ativ_circ_liq",
# "EV/EBIT":"ev_ebit",
# "EV/EBITDA":"ev_ebitda",
# "Mrg Ebit":"mrg_ebit",
# "Mrg. Líq.":"mrg_liq",
# "Liq. Corr.":"liq_corr",
# "ROIC":"roic",
# "ROE":"roe",
# "Liq.2meses":"liq_2meses",
# "Liq. Corr.":"liq_corr",
# "Patrim. Líq":"patrim_liq",
# "Dív.Brut/ Patrim.":"div_bruta_patrim",
# "Cresc. Rec.5a":"cresc_rec_cinco_anos"}
# fundamentus.rename(columns=columns_map,inplace=True)

# # removendo os sinais de porcentagem dos campos
# fundamentus['div_yield'] = fundamentus['div_yield'].apply(lambda x: str(x).strip(' %').replace('.','').replace(',','.')).astype(float) / 100
# fundamentus['roic'] = fundamentus['roic'].apply(lambda x: str(x).strip(' %').replace('.','').replace(',','.')).astype(float) / 100
# fundamentus['roe'] = fundamentus['roe'].apply(lambda x: str(x).strip(' %').replace('.','').replace(',','.')).astype(float) / 100
# fundamentus['mrg_ebit'] = fundamentus['mrg_ebit'].apply(lambda x: str(x).strip(' %').replace('.','').replace(',','.')).astype(float) / 100
# fundamentus['mrg_liq'] = fundamentus['mrg_liq'].apply(lambda x: str(x).strip(' %').replace('.','').replace(',','.')).astype(float) / 100
# fundamentus['cresc_rec_cinco_anos'] = fundamentus['cresc_rec_cinco_anos'].apply(lambda x: str(x).strip(' %').replace('.','').replace(',','.')).astype(float) / 100

# # ajustando os data types
# fundamentus['cotacao'] = fundamentus['cotacao'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['pl'] = fundamentus['pl'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['pvp'] = fundamentus['pvp'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['psr'] = fundamentus['psr'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['p_ativo'] = fundamentus['p_ativo'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['p_cap_giro'] = fundamentus['p_cap_giro'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['p_ebit'] = fundamentus['p_ebit'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['p_ativ_circ_liq'] = fundamentus['p_ativ_circ_liq'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['ev_ebit'] = fundamentus['ev_ebit'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['ev_ebitda'] = fundamentus['ev_ebitda'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['liq_2meses'] = fundamentus['liq_2meses'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['patrim_liq'] = fundamentus['patrim_liq'].str.replace('.', '').str.replace(',', '.').astype(float)
# fundamentus['div_bruta_patrim'] = fundamentus['div_bruta_patrim'].str.replace('.', '').str.replace(',', '.').astype(float)

# # salvando o parquet ajustado
# fundamentus.to_parquet('fundamentus.parquet')

tablenames = ['cad','dfp_cia_aberta_DRE_con_2023','fundamentei','fundamentus']

for name in tablenames:
    print(name)
    namestr = name+'.parquet'
    df = pd.read_parquet(namestr)
    # Conecta engine ***Atenção para o host*** ver docstring da classe mysql_etl
    engine = mysql_etl(
        host = 'host.docker.internal',
        user = 'root',
        password = 'root',
        database = 'db'
    ).criar()
    # # Carrega os dados tratados no banco Mysql
    df.to_sql(name, con=engine, if_exists='append', index=False)


# dtype_dfp_cia_aberta_DRE_con_2023 = {'DS_CONTA': 'VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'}
# dtype_fundamentei = {'reclameaquitexto': 'VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'}
# # Chamada das funções
# tablenames = ['cad','dfp_cia_aberta_DRE_con_2023','fundamentei','fundamentus']
# n = 0
# list_dtypes = [dtype_dfp_cia_aberta_DRE_con_2023,dtype_fundamentei]
# for name in tablenames:
#     print(name)
#     namestr = name+'.parquet'
#     df = pd.read_parquet(namestr)
#     # Conecta engine ***Atenção para o host*** ver docstring da classe mysql_etl
#     engine = mysql_etl(
#         host = 'host.docker.internal',
#         user = 'root',
#         password = 'root',
#         database = 'db'
#     ).criar()
#     # # Carrega os dados tratados no banco Mysql
#     df.to_sql(name, con=engine, if_exists='append', index=False,dtype=list_dtypes[n])
#     n += 1
    


    
    
