#!/usr/bin/env python

import redis
import os
import logging
from tempfile import NamedTemporaryFile
import subprocess
import time
import argparse

logging.basicConfig(level=logging.DEBUG)

CHMOD=' --chmod=ug+x,u+rw,g+r,g-w,o-rwx '

PASSWORD = None
if 'REDIS_CONFIG' in os.environ:
  PASSWORD = os.environ['REDIS_CONFIG'].split().pop(-1)


def get_files( client, redis_key, batch_size=50, exclude=['.xml',] ):
  transfer = {}
  for i in range( batch_size ):
    ret = client.blpop( redis_key, timeout=1 )
    if ret == None:
      break
    try:
      _, data = ret
      #logging.info("DATA: %s" % (data,))
      # always assume each line is  f'{filepath} -> {self.target}
      s, t = data.decode("utf-8").split( ' -> ' )
      if not t in transfer:
        transfer[t] = []
      add = True
      for f in exclude:
        if f in s:
          add = False
          break
      if add:
        transfer[t].append( s )
    except Exception as e:
      logging.warn("Error: %s" % (e,))
      pass
  return transfer


def copy( client, redis_key, batch_size=50, parallel=5, dry_run=True ):
  # write to temp file to pipe into parallel
  transfers = get_files( client, redis_key, batch_size=batch_size )
  #logging.info("TRANSFERS: %s" % (transfers,))
  for target, files in iter(transfers.items()):
    copied = []
    with NamedTemporaryFile(dir='/tmp', prefix='redis-copy.', delete=True ) as f:
      f.write( ( '\n'.join( files ) + '\n' ).encode(encoding='UTF-8') )
      f.flush()
      logging.info("TRANSFER: %s" % (files,))
      cmd = "cat %s | grep -vE '^$' | SHELL=/bin/sh parallel --gnu --linebuffer --jobs=%s 'rsync -av %s%s {} %s/{//}/'" % ( f.name, parallel, '--dry-run' if dry_run else '', CHMOD, target ) 
      logging.info(f">> {cmd}")
      out = subprocess.getoutput( cmd ) 
      logging.info( f"{out}" )
    # rsync will spit out filenames of stuff that copied, so we grep for these, and remove them from files
      for o in out.split('\n'):
        if 'sending incremental file list' in o \
          or ' bytes/sec' in o \
          or o == '' \
          or 'total size is ' in o:
          continue;
        #logging.info(" copied over: %s" % (o,) )
        copied.append( o )
    logging.info("COPIED: %s" % (copied,))


def main( source_dir='/input', redis_host='redis', redis_key='tem', redis_port=6373, redis_db=4, batch_size=50, sleep_time=1, dry_run=True, parallel=5, **kwargs ):

  if dry_run:
    logging.info("DRY RUN ENABLED")

  os.chdir( source_dir )
  if not os.path.exists( '.online' ):
    raise Exception(f"File system {source_dir} not mounted!")

  logging.info( f'connecting to redis server {redis_host}:{redis_port}/{redis_db}' ) # with {PASSWORD}' )
  client = redis.StrictRedis( host=redis_host, port=redis_port, db=redis_db, password=PASSWORD )

  while True:

    queued = client.llen( redis_key )
    logging.info('Queued files for transfer: %s' % (queued,))

    copy( client, redis_key, batch_size=batch_size, parallel=parallel, dry_run=dry_run )
    time.sleep( sleep_time )
    

if __name__ == "__main__":

  parser = argparse.ArgumentParser(description='CryoEM Data Mover')
  parser.add_argument('--source_dir', help='directory to copy from', default=os.environ['SOURCE_DIRECTORY'] )
  parser.add_argument('--redis_key', help='redis key for files', default=os.environ['REDIS_KEY'] )
  parser.add_argument('--redis_host', help='address of redis server', default=os.environ['REDIS_SERVICE_HOST'] )
  parser.add_argument('--redis_port', type=int, help='host port of redis server', default=os.environ['REDIS_SERVICE_PORT'] )
  parser.add_argument('--redis_db', type=int, help='redis database', default=os.environ['REDIS_RSYNC_DB'] if 'REDIS_RSYNC_DB' in os.environ else 4)
  parser.add_argument('--redis_password', help='redis password', default=PASSWORD)
  parser.add_argument('--sleep_time', type=int, help='wait time after file completes to copy again', default=1 )
  parser.add_argument('--parallel', type=int, help='number of parallel transfers to use', default=5 )
  parser.add_argument('--dry_run', help='do not actualy transfer any files', default=bool(os.environ['DRY_RUN']) if 'DRY_RUN' in os.environ else False, action='store_true' )

  args = parser.parse_args()

  #main( args.directory, args.redis_host, args.redis_key, **vars(args) )
  main( **vars(args) )


