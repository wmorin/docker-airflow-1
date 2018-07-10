from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator

from airflow.operators.python_operator import ShortCircuitOperator

from builtins import bytes

from subprocess import Popen, STDOUT, PIPE, call
from tempfile import gettempdir, NamedTemporaryFile
from airflow.utils.file import TemporaryDirectory

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory

import ast
import glob
import shutil
import os
import hashlib
from datetime import datetime, timedelta, timezone

import shutil
from pathlib import Path
from ast import literal_eval

import logging

LOG = logging.getLogger(__name__)

class FileGlobSensor(BaseSensorOperator):
    template_fields = ( 'filepath', )
    ui_color = '#b19cd9'
    @apply_defaults
    def __init__(self, filepath, extensions=[], excludes=[], timeout=60, soft_fail=False, poke_interval=5, provide_context=False, recursive=False, *args, **kwargs):
        super(FileGlobSensor, self).__init__(poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, *args, **kwargs)
        self.filepath = filepath
        self.recursive = recursive
        self.extensions = extensions
        self.excludes = excludes
    def poke(self, context):
        LOG.info('Waiting for file %s (ext %s)' % (self.filepath,self.extensions) )
        files = []
        for f in glob.iglob( self.filepath, recursive=self.recursive ):
            add = False
            if self.extensions:
                for e in self.extensions:
                    if f.endswith( e ):
                        add = True
            else:
                add = True
            if add:
                if self.excludes:
                    for e in self.excludes:
                        if e in f:
                            add = False
                if add:
                    files.append(f)
        if len(files):
            LOG.info('found files: %s' % (files) )
            context['task_instance'].xcom_push(key='return_value',value=files)
            return True
        return False



class FileInfoSensor(BaseSensorOperator):
    template_fields = ( 'filepath', )
    ui_color = '#b19cd9'
    @apply_defaults
    def __init__(self, filepath, extensions=[], excludes=[], timeout=60, soft_fail=False, poke_interval=5, provide_context=False, recursive=False, *args, **kwargs):
        super(FileInfoSensor, self).__init__(poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, *args, **kwargs)
        self.filepath = filepath
        self.recursive = recursive
        self.extensions = extensions
        self.excludes = excludes
    def poke(self, context):
        LOG.info('Waiting for file %s (ext %s)' % (self.filepath,self.extensions) )
        files = []
        for f in glob.iglob( self.filepath, recursive=self.recursive ):
            add = False
            if self.extensions:
                for e in self.extensions:
                    if f.endswith( e ):
                        add = True
            else:
                add = True
            if add:
                if self.excludes:
                    for e in self.excludes:
                        if e in f:
                            add = False
                if add:
                    files.append(f)
        if len(files):
            LOG.info('found files: %s' % (files) )
            context['task_instance'].xcom_push(key='return_value',value=files)
            info = []
            for f in files:
                data = {}
                # TODO: strip absolute path to relative path
                data['path'] = f.replace('/gpfs/slac/cryo/fs1/exp//','')
    
                # get hash    
                hash_md5 = hashlib.md5()
                with open(f, "rb") as stream:
                    for chunk in iter(lambda: stream.read(65536), b""):
                        hash_md5.update(chunk)
                    data['checksum'] = hash_md5.hexdigest()
    
                # get other
                def ts( epoch ):
                    dt = datetime.fromtimestamp(epoch).replace(microsecond=0)
                    return dt.replace(tzinfo=timezone.utc).isoformat()[:-6] + 'Z'
    
                data['modify_timestamp'] = ts( os.path.getmtime( f ) )
                data['create_timestamp'] = ts( os.path.getctime( f ) )
                data['size'] = os.path.getsize( f )
                info.append( data )
            context['task_instance'].xcom_push(key='info',value=info)
                
            return True
        return False
           


class FileSensor(BaseSensorOperator):
    template_fields = ( 'filepath', )
    ui_color = '#b19cd9'
    @apply_defaults
    def __init__(self, filepath, timeout=60, soft_fail=False, poke_interval=5, provide_context=False, *args, **kwargs):
        super(FileSensor, self).__init__(poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, *args, **kwargs)
        self.filepath = filepath
    def poke(self, context):
        LOG.info('Waiting for file %s' % (self.filepath,) )
        if os.path.exists( self.filepath ):
            context['task_instance'].xcom_push(key='return_value',value=self.filepath)
            return True
        return False


def ensureDirectoryExists(**kwargs):
    LOG.info("Checking directory %s" % (kwargs['directory'],))
    if not os.path.exists(kwargs['directory']):
        try:
            os.makedirs(kwargs['directory'])
        except Exception as e:
            raise AirflowException('Error creating destination directory: %s' % (e,))
    return kwargs['directory']
class EnsureDirectoryExistsOperator(ShortCircuitOperator):
    """ will create directories specified if it doesn't already exist """
    ui_color = '#b19cd9'
    def __init__(self,directory,*args,**kwargs):
        super(EnsureDirectoryExistsOperator,self).__init__(python_callable=ensureDirectoryExists, op_kwargs={'directory': directory}, *args, **kwargs)



class FileOperator(BaseOperator):
    ui_color = '#b19cd9'
    @apply_defaults
    def __init__(self,source,destination, *args, **kwargs):
        super(FileOperator, self).__init__(*args,**kwargs)
        self.src = source
        self.dst = destination
    def execute(self, context):
        self.log.info('Moving file from %s to %s' % (self.src, self.dst))
        try:
            shutil.move( self.src, self.dst )
            return self.dst
        except Exception as e:
            raise AirflowException('Error moving file: %s' % e)


class RsyncOperator(BaseOperator):
    """
    Execute a rsync
    """
    template_fields = ('env','source','target','includes','dry_run','newer')
    template_ext = ( '.sh', '.bash' )
    ui_color = '#f0ede4'
    
    @apply_defaults
    def __init__(self, source, target, newer=None, newer_offset='10 mins', xcom_push=True, env=None, output_encoding='utf-8', prune_empty_dirs=False, parallel=2, includes='', excludes='', flatten=False, dry_run=False, chmod='ug+x,u+rw,g+r,g-w,o-rwx', *args, **kwargs ):
        super(RsyncOperator, self).__init__(*args,**kwargs)
        self.env = env
        self.output_encoding = output_encoding
        
        self.source = source
        self.target = target
        
        self.includes = includes
        self.excludes = excludes
        self.prune_empty_dirs = prune_empty_dirs
        self.flatten = flatten
        self.dry_run = dry_run
        self.chmod = chmod
        self.parallel = parallel

        self.xcom_push_flag = xcom_push
        
        self.newer = newer
        self.newer_offset = newer_offset
        
        self.rsync_command = ''
        
    def execute(self, context):
                
        def build_find_files( input, exclude=False ):
            out = ''
            try:
                a = input
                if isinstance(input, str):
                    a = ast.literal_eval(input)
                array = [ "%s -name '%s'" % ('!' if exclude else '', i) for i in a ]
                out = ' -o '.join(array)
            except:
                if input:
                    out = " %s -name '%s'" % ('!' if exclude else '', input)
            return out

        output = []
        # LOG.info("tmp dir root location: " + gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                find_arg = build_find_files( self.excludes, exclude=True ) + build_find_files( self.includes )
                dry = True if self.dry_run.lower() == 'true' else False

                newer = None
                if self.newer and not self.newer == "None":
                    newer = 'date -d "$(date -r ' + self.newer + ') - ' + self.newer_offset + '" +"%Y-%m-%d %H:%M:%S"'

                # format rsync command
                rsync_command = """
                    rsync -a --exclude='$RECYCLE.BIN'  --exclude='System Volume Information' -f'+ */' -f'- *' %s %s %s && \
                    cd %s && \
                    find . -type f \( %s \) %s | grep -v '$RECYCLE.BIN' | SHELL=/bin/sh parallel --gnu --linebuffer --jobs=%s 'rsync -av %s%s%s {} %s/{//}/'
                    find %s -type d -empty %s
                    """ % ( \
                        # sync directories
                        ' --dry-run' if dry else '',
                        self.source,
                        self.target,
                        # cd
                        self.source,
                        # find
                        find_arg,
                        ' -newermt "`%s`"'%(newer,) if newer else '',
                        # parallel
                        self.parallel,
                        # rsync files
                        ' --dry-run' if dry else '', \
                        ' --chmod=%s' % (self.chmod,) if self.chmod else '', \
                        ' -d --no-relative' if self.flatten else '', \
                        self.target,
                        # delete empty directories
                        self.target,
                        '' if dry else ' -delete',
                 )


                f.write(bytes(rsync_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script "
                             "location :{0}".format(script_location))
                logging.info("Running rsync command: " + rsync_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env)

                self.sp = sp

                logging.info("Output:")
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).strip()
                    # parse for file names here
                    if line.startswith( 'building file list' ) or line.startswith( 'sent ') or line.startswith( 'total size is ' ) or line.startswith('sending incremental file list') or '/sec' in line or 'speedup is ' in line or 'created directory ' in line or line in ('', './'):
                        continue
                    else:
                        LOG.info(line)
                        output.append( line )
                sp.wait()
                logging.info("Command exited with "
                             "return code {0}".format(sp.returncode))

                if sp.returncode:
                    raise AirflowException("rsync command failed")

        if self.xcom_push_flag:
            return output

        
    def on_kill(self):
        LOG.info('Sending SIGTERM signal to bash subprocess')
        self.sp.terminate()


    

class ExtendedAclOperator(BaseOperator):
    """ match the fs acls to that provided """
    template_fields = ('directory','users')
    def __init__(self,directory,users=[],env=None,*args,**kwargs):
        super(ExtendedAclOperator,self).__init__(*args,**kwargs)
        self.env = env
        self.directory = directory
        self.users = users

    def do(self, command, output_encoding='utf-8'):
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script "
                             "location :{0}".format(script_location))
                logging.info("Running command: " + command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env)
                self.sp = sp
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(output_encoding).strip()
                    yield line
                sp.wait()
        self.sp.terminate()
        return

    def execute( self, context):
        
        acls = [ l for l in self.do( "getfacl %s" % self.directory ) ]
        # logging.info("ACLS: %s" % acls)
        current_uids = [ u.split(':')[1] for u in acls if u.startswith('user:') and not '::' in u ]
        
        if isinstance(self.users,str):
            self.users = literal_eval(self.users)
        logging.info("Set ACL %s -> %s" % (current_uids, self.users,))

        did_something = False

        add_users = set(self.users) - set(current_uids)
        for u in add_users:
            # set recursive
            cmd = "setfacl -Rm u:%s:rx %s" % (u, self.directory)
            if not call( cmd.split() ) == 0:
                raise Exception("Could not set recursive acls %s" % (cmd,) )
            cmd = "setfacl -R -d -m u:%s:rx %s" % (u, self.directory) 
            if not call( cmd.split() ) == 0:
                raise Exception("Could not set default acls %s" % (cmd,) )
            did_something = True
        
        del_users = set(current_uids) - set(self.users)
        for u in del_users:
            # del user from acl
            cmd = "setfacl -Rx u:%s %s" % (u, self.directory) 
            if not call( cmd.split() ) == 0:
                raise Exception("Could not remove recursive acl %s" % (cmd,) )
            # del default
            cmd = "setfacl -R -d -x u:%s %s" % (u, self.directory)
            if not call( cmd.split() ) == 0:
                raise Exception("Could not remove default acl %s" % (cmd,) )
            did_something = True

        if did_something:
            # make sure we don't let others read
            cmd = "setfacl -Rm o::--- %s" % (self.directory,)
            if not call( cmd.split() ) == 0:
                raise Exception("Could not restrict others acl %s" % (cmd,) )
            # default others
            cmd = "setfacl -R -d m o::--- %s" % (self.directory,)
            if not call( cmd.split() ) == 0:
                raise Exception("Could not restrict others acl %s" % (cmd,) )


    def on_kill(self):
        LOG.info('Sending SIGTERM signal to bash subprocess')
        self.sp.terminate()





class FilePlugin(AirflowPlugin):
    name = 'file_plugin'
    operators = [FileGlobSensor,EnsureDirectoryExistsOperator,FileOperator,RsyncOperator,FileSensor,ExtendedAclOperator,FileInfoSensor]
