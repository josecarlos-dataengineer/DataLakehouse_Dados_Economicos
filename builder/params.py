# ----------------------------------------------------------------------------
# ---------------------- Definição de variáveis ------------------------------
sufixes = {'s3':'727477891012','gcp':'99999999','azure':'9999999999'}

prefixes = 'de'

layers = {'datalake':{
            '1':'landing',
            '2':'processed',
            '3':'curated',
            '4':'consume'},
          'datalakehouse':{
            '1':'bronze',
            '2':'silver',
            '3':'gold'}}

enviroments = {'prod':'prod',
               'dev':'dev',
               'stg':'staging'}

projects = {'default':'default',
            'case':'case',
            'test':'test'}

providers = {'s3':'s3',
             'gcp':'gcp',
             'azure':'azure',
             'local':'local',
             'default':'default'}

profiles = {'admin':'admin',
            'data_engineer':'data_engineer',
            'tester':'tester'}




