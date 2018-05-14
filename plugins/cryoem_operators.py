
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

import json
import yaml


import logging
LOG = logging.getLogger(__name__)


###
# grab some experimental details that we need ###
###
def parseConfigurationFile(**kwargs):
    with open( kwargs['configuration_file'], 'r' ) as stream:
        try:
            d = yaml.load(stream)
            for k,v in d.items():
                kwargs['task_instance'].xcom_push(key=k,value=v)
            exp_dir = '%s_%s' % (d['experiment']['name'], d['experiment']['microscope'])
            kwargs['task_instance'].xcom_push(key='experiment_directory', value='%s/%s/' % ( kwargs['destination_directory'], exp_dir))
            kwargs['task_instance'].xcom_push(key='dry_run', value=d['experiment']['dry_run'] if 'dry_run' in d['experiment'] else True)
        except Exception as e:
            raise AirflowException('Error creating destination directory: %s' % (e,))
    return kwargs['configuration_file']
class EnsureConfigurationExistsSensor(ShortCircuitOperator):
    """ monitor for configuration defined on tem """
    def __init__(self,configuration_file,destination_directory,*args,**kwargs):
        super(EnsureConfigurationExistsSensor,self).__init__(
            python_callable=parseConfigurationFile, 
            op_kwargs={ 'configuration_file': configuration_file, 'destination_directory': destination_directory }, 
            *args, **kwargs)


    
def logbook_configuration(**kwargs):

    http_hook = kwargs['http_hook']
    microscope = kwargs['microscope']
    experiment_directory = kwargs['base_directory']
    
    # get instrument info
    r = http_hook.run( '/cryoem-data/lgbk/ws/instruments' )
    if r.status_code in (403,404,500):
        logging.error(" could not fetch instruments: %s" % (r.text,))
        return False
    # logging.info("instruments: %s" % i )
    found = False
    for inst in json.loads( r.text )['value']:
        if '_id' in inst and microscope.lower() == inst['_id'].lower():
            kwargs['task_instance'].xcom_push(key='instrument', value=inst)
            found = True
    if not found:
        logging.error(" could not find instrument %s" % (microscope,))
        return False
        
    # get experiment and sample
    r = http_hook.run( '/cryoem-data/lgbk/ws/activeexperiments' )
    if r.status_code in (403,404,500):
        logging.error(" could not fetch active experiment: %s" % (r.text,))
        return False

    active = json.loads( r.text )
    if active['success']:
        # print("Active experiments: %s" % active['value'])
        for tem in active['value']:
            # print("Found %s" % (tem,))
            if 'instrument' in tem and tem['instrument'].lower() == microscope.lower():
                # logging.info("found %s: %s" % (args['tem'],tem))

                ###
                # get the active experiment and sample
                ###
                experiment_name = tem['name']
                s = http_hook.run( '/cryoem-data/lgbk/%s/ws/current_sample_name' % ( experiment_name, ) )
                sample_name = json.loads( s.text )['value']
                logging.info("active sample: %s" % (sample_name,))
                if sample_name == None:
                    logging.error("No active sample on %s" % (tem['name'],))
                    return False
                    
                # get sample parameters
                p = http_hook.run( '/cryoem-data/lgbk/%s/ws/samples' % ( experiment_name, ) )
                sample_params = {}
                sample_guid = None
                for d in json.loads( p.text )['value']:
                    # logging.info("SAMPLE: %s" % (d,))
                    if d['name'] == sample_name and d['current']:
                        sample_params = d['params']
                        sample_guid = d['_id']
                kwargs['task_instance'].xcom_push(key='sample', value={
                    'name': sample_name,
                    'guid': sample_guid,
                    'params': sample_params
                } )
                
                parent_fileset = experiment_name[:6]
                experiment_directory = experiment_directory + '/' + parent_fileset + '/' + experiment_name
                kwargs['task_instance'].xcom_push(key='experiment', value=experiment_name )
                kwargs['task_instance'].xcom_push(key='experiment_directory', value=experiment_directory )
                
                ###
                # get list of collaborators for file permissions
                ###
                a = http_hook.run( '/cryoem-data/lgbk/%s/ws/collaborators' % (experiment_name, ) )
                collaborators = json.loads( a.text )
                logging.info( " collaborators: %s" % (collaborators,))
                kwargs['task_instance'].xcom_push(key='collaborators', value=[ u['uidNumber'] for u in collaborators['value'] if u['is_group'] == False ] if 'value' in collaborators else [] )
                
                return True
                

class LogbookConfigurationSensor(ShortCircuitOperator):
    """ monitor for configuration defined on tem """
    def __init__(self,http_hook,microscope,base_directory,*args,**kwargs):
        super(LogbookConfigurationSensor,self).__init__(
            python_callable=logbook_configuration, 
            op_kwargs={ 'http_hook': http_hook, 'microscope': microscope, 'base_directory': base_directory }, 
            *args, **kwargs)






class CryoEMPlugin(AirflowPlugin):
    name = 'cryoem_plugin'
    operators = [EnsureConfigurationExistsSensor,LogbookConfigurationSensor]
