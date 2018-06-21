
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.models import BaseOperator

from airflow.exceptions import AirflowException, AirflowSkipException

import json
import yaml
from pathlib import Path
import re

from datetime import datetime, timedelta
from datetime import timezone

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
    
    http_hook.method = 'GET'
    
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
                if not 'name' in tem:
                    return False                    
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
                experiment_directory = experiment_directory + '/' + parent_fileset + '/' + experiment_name + '_' + microscope.upper()
                kwargs['task_instance'].xcom_push(key='experiment', value=experiment_name + '_' + microscope.upper() )
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



# this shoudl probably be run as part of teh daq
#class LogbookCreateRunOperator(BaseOperator):
#    """ monitor for configuration defined on tem """
#    template_fields = ('experiment','run')
#    
#    def __init__(self,http_hook,experiment,run,*args,**kwargs):
#        super(LogbookCreateRunOperator,self).__init__(*args, **kwargs)
#        self.experiment = experiment
#        self.run = run
#        self.http_hook = http_hook
#
#    def execute(self, context): 
#        self.http_hook.method = 'GET'
#        r = self.http_hook.run( '/cryoem-data/run_control/%s/ws/start_run?run_num=%s' % (self.experiment, self.run ) )
#        if r.status_code in (403,404,500):
#            logging.error(" could not initiate run %s for experiment %s: %s" % (self.run, self.experiment, r.text,))
#            return False
#        return True


class LogbookCreateRunOperator(BaseOperator):
    template_fields = ('experiment',)
    def __init__(self,http_hook,experiment,from_task='rsync',from_key='return_value',*args,**kwargs):
        super(LogbookCreateRunOperator,self).__init__(*args, **kwargs)
        self.http_hook = http_hook
        self.experiment = experiment
        self.from_task = from_task
        self.from_key = from_key
    def execute(self, context):
        found = {}
        self.http_hook.method = 'GET'
        for f in context['task_instance'].xcom_pull(task_ids=self.from_task,key=self.from_key):
            this = Path(f).resolve().stem
            for pattern in ( r'\-\d+$', r'\-gain\-ref$' ):
                if re.search( pattern, this):
                    this = re.sub( pattern, '', this)
                # LOG.warn("mapped: %s -> %s" % (f, this))
            #LOG.info("this: %s, f: %s" % (this,f))
            # EPU: only care about the xml file for now (let the dag deal with the other files
            if f.endswith('.xml') and not f.startswith('Atlas') and not f.startswith('Tile_') and '_Data_' in f:
                #LOG.warn("found EPU metadata %s" % this )
                found[this] = True
            # serialEM: just look for tifs
            elif f.endswith('.tif'):
                m = re.match( r'^(?P<base>.*\_\d\d\d\d\d)(\_.*)?\.tif$', f )
                if m:
                    #LOG.info('found %s' % (m.groupdict()['base'],) )
                    found[m.groupdict()['base']] = True
        if len(found.keys()) == 0:
            raise AirflowSkipException('No runs')

        self.http_hook.method = 'GET'
        for base_filename,_ in sorted(found.items()):
            r = self.http_hook.run( '/cryoem-data/run_control/%s/ws/start_run?run_num=%s' % (self.experiment, base_filename ) )
            LOG.warn('run %s: %s' % (base_filename,r.text)) 
            if r.status_code in (403,404,500):
                LOG.error(" could not initiate run %s for experiment %s: %s" % (base_filename, self.experiment, r.text,))

class LogbookRegisterFileOperator(BaseOperator):
    """ register the file information into the logbook """
    template_fields = ('experiment','run')
    
    def __init__(self,http_hook,file_info,experiment,run,index=0,*args,**kwargs):
        super(LogbookRegisterFileOperator,self).__init__(*args, **kwargs)
        self.experiment = experiment
        self.run = run
        self.http_hook = http_hook
        self.file_info = file_info
        self.index = index

    def execute(self, context): 

        data = context['task_instance'].xcom_pull( task_ids=self.file_info, key='info' )[self.index]
        data['run_num'] = self.run
        
        self.http_hook.method = 'POST'
        r = self.http_hook.run( '/cryoem-data/lgbk/%s/ws/register_file' % (self.experiment,), data=json.dumps([data,]), headers={'content-type': 'application/json'} )
        if r.status_code in (403,404,500):
            logging.error(" could not register file %s on run %s for experiment %s: %s" % (self.file_info, self.run, self.experiment, r.text,))
            return False
        return True
    

class LogbookRegisterRunParamsOperator(BaseOperator):
    """ register the run information into the logbook """
    template_fields = ('experiment','run')
    
    def __init__(self,http_hook,experiment,run,*args,**kwargs):
        super(LogbookRegisterRunParamsOperator,self).__init__(*args, **kwargs)
        self.experiment = experiment
        self.run = run
        self.http_hook = http_hook

    def execute(self, context): 

        data = {}
        for k,v in context['task_instance'].xcom_pull( task_ids='drift_data', key='return_value' ).items():
            data[k] = v
        for task in ( 'summed_ctf_data', 'aligned_ctf_data' ):
            prefix = 'unaligned' if task == 'summed_ctf_data' else 'aligned'
            for k, v in context['task_instance'].xcom_pull( task_ids=task, key='context' ).items():
                if k == 'pixel_size':
                    k = '%s_pixel_size' % prefix
                data[k] = v
            for k, v in context['task_instance'].xcom_pull( task_ids=task, key='return_value' ).items():
                data['%s_%s' % (prefix,k)] = v

        data['preview'] = context['task_instance'].xcom_pull( task_ids='previews_file', key='info' )[0]['path']
        
        # data['run_num'] = self.run
        LOG.warn("DATA: %s" % data )

        self.http_hook.method = 'POST'
        r = self.http_hook.run( '/cryoem-data/run_control/%s/ws/add_run_params?run_num=%s' % (self.experiment,self.run), data=json.dumps(data), headers={'content-type': 'application/json'} )
        if r.status_code in (403,404,500):
            logging.error(" could not register run params on run %s for experiment %s: %s" % (self.run, self.experiment, r.text,))
            return False
        return True






class CryoEMPlugin(AirflowPlugin):
    name = 'cryoem_plugin'
    operators = [EnsureConfigurationExistsSensor,LogbookConfigurationSensor,LogbookCreateRunOperator,LogbookRegisterFileOperator,LogbookRegisterRunParamsOperator]
