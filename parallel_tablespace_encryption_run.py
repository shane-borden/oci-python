#!/usr/bin/python

import cx_Oracle, time, os, logging, random, sys
import multiprocessing as mp
import datetime
from concurrent.futures import ProcessPoolExecutor as PoolExecutor

def encrypt_proc(args):
   file_name_arg = args
   begin_time = datetime.datetime.now()
   con = cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
   cur = con.cursor()
   sql_text = 'alter session set container=' + pdb_name
   cur.execute(sql_text)
   sql_text = 'alter database datafile \'' + file_name_arg + '\' encrypt'
   try:
     cur.execute(sql_text)
   except cx_Oracle.DatabaseError as e:
     error, = e.args
     if error.code == 28431:
       print('Datafile: ' + file_name_arg + ' already encrypted')
       logging.info('Datafile: ' + file_name_arg + ' already encrypted')
     elif error.code == 28440:
       print('Datafile: ' + file_name_arg + ' is in use or not offline')
       logging.info('Datafile: ' + file_name_arg + ' is in use or not offline')
     else:
       print('**ERROR on Datafile: ' + file_name_arg + ' Error: ' + str(error.message))
       logging.info('Datafile: ' + file_name_arg + ' Error: ' + str(error.code) + '  :  ' + error.message + '  :  ' + error.context)
   cur.close()
   con.close()
   total_time = datetime.datetime.now() - begin_time
   if total_time.seconds > 0:
     logging.info('Finished encryption of \'' + sql_text + '\' in ' + str(total_time.seconds) + ' seconds')
   else:
     logging.info('Finished encryption of \'' + sql_text + '\' in ' + str(total_time.microseconds) + ' microsecond')
   return

if __name__ == '__main__':
  log_date = datetime.datetime.now().strftime("%m%d%Y%H%M")
  print('****** Ensure you have the correct environment sourced in BEFORE PROCEEDING ******')
  pdb_name = str(raw_input("Enter PDB you wish to encrypt or Cntrl-C to exit: "))
  pdb_name = pdb_name.upper()
  num_threads = int(raw_input("Enter the number of threads you want to execute: "))
  num_threads = min(num_threads, mp.cpu_count())
  file_name_array = []

  logpath = os.path.dirname(os.path.realpath(__file__)) + '/log'
  if not os.path.isdir(logpath):
    print ('Log directory does not exist - creating directory: ' + logpath)
    try:
      os.mkdir(logpath)
    except OSError:
      print ("Creation of the directory %s failed" % logpath)
      sys.exit()
    else:
      print ("Successfully created the directory %s " % logpath)

  logging.basicConfig(filename='./log/' + pdb_name + '_ts_encryption_' + log_date + '.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
  logging.info('Script for ' + pdb_name + ' began')
  logging.info('Executing ' + str(num_threads) + ' threads')
  con = cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
  cur = con.cursor()
  sql_text = 'select df.name from v$tablespace ts, v$datafile df, v$pdbs pdb \n'
  sql_text = sql_text + 'where ts.ts#=df.ts# and ts.con_id=df.con_id and (ts.name not in (\'SYSTEM\',\'SYSAUX\') \n'
  sql_text = sql_text + 'and ts.name not in (select value from gv$parameter where name=\'undo_tablespace\')) \n'
  sql_text = sql_text + 'and df.con_id=pdb.con_id and ts.name not like \'UNDO%\' \n'
  sql_text = sql_text + 'and pdb.name = \'' + pdb_name + '\' order by df.con_id, df.file#'
  try:
    cur.execute(sql_text)
  except cx_Oracle.DatabaseError as e:
    error, = e.args
    print('**SQL ERROR: ' + file_name_arg + ' Error: ' + error.code + '  :  ' + error.message + '  :  ' + error.context)
    logging.info('**SQL ERROR: ' + file_name_arg + ' Error: ' + error.code + '  :  ' + error.message + '  :  ' + error.context)
  for result in cur:
    file_name_array.append(str(result).strip('\'(),'))
  cur.close()
  con.close()

  logging.info('Potiential files to be encrypted:')
  for result in file_name_array:
    print(result)
    logging.info('   ' + result)

  print('Starting Parallel Worker Threads.....')
  logging.info('Starting Parallel Worker Threads.....')

  print(str(len(file_name_array)) + ' Tablespaces to Encrypt')
  total_tablespaces_remaining = len(file_name_array)
  logging.info(str(len(file_name_array)) + ' Tablespaces to Encrypt')

  with PoolExecutor(max_workers=num_threads) as executor:
    for _ in executor.map(encrypt_proc, file_name_array):
      pass
      total_tablespaces_remaining = total_tablespaces_remaining - 1
      print('Tablespaces remaining to encrypt: ' + str(total_tablespaces_remaining))

  logging.info('Script for ' + pdb_name + ' completed')
  print('Script for ' + pdb_name + ' completed')