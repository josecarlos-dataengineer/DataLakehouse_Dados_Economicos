import logging
from params import *

# ----------------------------------------------------------------------------
# ---------------------- Data Quality ----------------------------------------
class qualitiy_check():    
    '''
    This classe aims to perform quality check on directory building    
    '''
    
    def params_check(is_cloud:bool,frequency:str,schedule:bool,provider=None,
        layer=None,arquitecture=None,sufix=None,prefix=None):
        '''
        Função checa se os parâmetros inseridos na builder.mount() foram 
        inseridos conforme esperado.
        '''
            
        if is_cloud == 0:
            try:
                assert is_cloud in [0,1]
                logging.info('Valid is_cloud parameter')
                assert frequency in ['daily','hourly','monthly']
                logging.info('Valid frequency')
                assert schedule in [0,1]
                logging.info('Valid schedule parameter') 

            except Exception as e:
                logging.error('One of the mandatory parameters is missing.')  

        elif is_cloud == 1:
            
            try:                    
                assert is_cloud in [0,1]
                logging.info('Valid is_cloud parameter')
                assert frequency in ['daily','hourly','monthly']
                logging.info('Valid frequency')
                assert schedule in [0,1]
                logging.info('Valid schedule parameter') 
                assert provider in providers.keys()
                logging.info('Valid provider')
                assert layer in ['1','2','3','4']
                logging.info('Valid layer')
                assert arquitecture in layers.keys()
                logging.info('Valid arquitecture')   
                assert sufix in sufixes.keys()
                logging.info('Valid Sufix')
                assert prefix == prefixes
                logging.info('Valid prefix')
                assert arquitecture in layers.keys()
                logging.info('Valid arquitecture')
                assert layer in ['1','2','3','4']
                logging.info('Valid layer')
            
            
            except:
                error_msg = '''One of the non mandatory parameters is missing.'''
                logging.error(error_msg)