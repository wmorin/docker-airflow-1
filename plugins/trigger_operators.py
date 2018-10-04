

from airflow.plugins_manager import AirflowPlugin

from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.exceptions import AirflowException, AirflowSkipException

from airflow import settings
import contextlib


from airflow.models import DagRun, DagBag
from airflow.utils.state import State

from pathlib import Path
from datetime import datetime
from time import sleep
import re
from distutils.util import strtobool

import logging
LOG = logging.getLogger(__name__)


def trigger_preprocessing(context):
    """ calls the preprocessing dag: pass the filenames of the stuff to process """
    # we have a jpg, xml, small mrc and large mrc, and gainref dm4 file
    # assume the common filename is the same and allow the  preprocessing dag wait for the other files? what happens if two separate calls to the same dag occur?
    found = {}
    if context == None:
        return

    for f in context['ti'].xcom_pull( task_ids='rsync', key='return_value' ):
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
        # tomography file
        elif '[' in this and ']' in this:
            t = this.split(']')[0] + ']'
            found[t] = True

    for base_filename,_ in sorted(found.items()):
        sample = context['ti'].xcom_pull( task_ids='config', key='sample' )
        inst = context['ti'].xcom_pull( task_ids='config', key='instrument' )
        name = context['ti'].xcom_pull( task_ids='config', key='experiment' )

        run_id = '%s__%s' % (name, base_filename)
        dro = DagRunOrder(run_id=run_id)

        d = sample['params']

        d['directory'] = context['ti'].xcom_pull( task_ids='config', key='experiment_directory' ) + '/' + sample['guid'] + '/'
        d['base'] = base_filename
        d['experiment'] = name
        d['microscope'] = inst['_id']
        d['cs'] = inst['params']['cs']
        d['keV'] = inst['params']['keV']

        # only do single-particle 
        if bool(strtobool(str(d['preprocess/enable']))) and d['imaging_method'] in ( 'single-particle', ):
            LOG.info('triggering dag %s with %s' % (run_id,d))
            dro.payload = d
            yield dro
    return


def trigger_null(context):
    raise AirflowSkipException('Intentionally not doing it')

@contextlib.contextmanager
def create_session():
    session = settings.Session()
    try:
        yield session
        session.expunge_all()
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

class TriggerMultipleDagRunOperator(TriggerDagRunOperator):
    template_fields = ('trigger_dag_id', 'dry_run' )
    def __init__(self, *args,**kwargs):
        self.dry_run = kwargs['dry_run']
        kwargs['python_callable'] = trigger_preprocessing
        super( TriggerMultipleDagRunOperator, self ).__init__( *args, **kwargs )
    def execute(self, context):
        count = 0
        self.python_callable = trigger_preprocessing
        dry = True if self.dry_run.lower() == 'true' else False
        for dro in self.python_callable(context):
            # LOG.info("context: %s" % (context,))
            # LOG.info(' TRIGGER %s' % (self.trigger_dag_id,))
            #if dro and ( not 'dry_run' in context['params'] or ( 'dry_run' in context['params'] and not bool(context['params']['dry_run'])) ):
            if dro and not dry:
                try:
                    with create_session() as session:
                        dbag = DagBag(settings.DAGS_FOLDER)
                        trigger_dag = dbag.get_dag(self.trigger_dag_id)
                        dr = trigger_dag.create_dagrun(
                            run_id=dro.run_id,
                            state=State.RUNNING,
                            conf=dro.payload,
                            external_trigger=True)
                        # LOG.info("Creating DagRun %s", dr)
                        session.add(dr)
                        session.commit()
                        count = count + 1
                except Exception as e:
                    LOG.error("Could not add %s: %s" % (dro.run_id,e))
            else:
                LOG.info("skipping due to dry run")
        if count == 0:
            raise AirflowSkipException('No external dags triggered')


class TriggerPlugin(AirflowPlugin):
    name = 'trigger_plugin'
    operators = [TriggerMultipleDagRunOperator,]
