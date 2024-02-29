import datetime as dt
import uuid
import pandas as pd


    
def create_metadata(df:pd.DataFrame):
    
    df['data_carga'] = dt.date.today()
    df['run_id'] = uuid.uuid4().hex[:8]
    print('metadados inseridos')
    return None


