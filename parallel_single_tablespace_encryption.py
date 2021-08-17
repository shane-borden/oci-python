#!/usr/bin/python3

import cx_Oracle, time, os, logging, random, sys
import multiprocessing as mp
import datetime
from concurrent.futures import ProcessPoolExecutor as PoolExecutor
import getpass

def encrypt_proc(args):
   file_name_arg = args
   begin_time = datetime.datetime.now()
   con = cx_Oracle.connect(user="sys", password=sysdba_password,
                           dsn=connection_string,
                           mode = cx_Oracle.SYSDBA)
   cur = con.cursor()
   if pdb_name:
      sql_text = 'alter session set container=' + pdb_name
      cur.execute(sql_text)
      sql_text = 'alter database datafile \'' + file_name_arg + '\' encrypt'
   else:
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
     logging.info('Finished encryption of file \'' + file_name_arg + '\' in ' + str(total_time.seconds) + ' seconds')
   else:
     logging.info('Finished encryption of file \'' + file_name_arg + '\' in ' + str(total_time.microseconds) + ' microsecond')
   return

if __name__ == '__main__':
  log_date = datetime.datetime.now().strftime("%m%d%Y%H%M")
  print('****** Ensure you have the correct environment sourced in BEFORE PROCEEDING ******')
  pdb_name = str(input("Enter PDB you wish to encrypt: ")).upper()
  tablespace_name = str(input("Enter the tablespace name you wish to encrypt: ")).upper()
  num_threads = int(input("Enter the number of threads you want to execute or enter for default of 1: ") or "1")
  num_threads = min(num_threads, mp.cpu_count())
  host_name = str(input("Enter the host name where the database resides.  Default localhost: ") or "localhost").lower()
  port_number = str(input("Enter the port number if different than the default port of 1521: ") or "1521")
  service_name = str(input("Enter the service name to connect to: ")).lower()
  sysdba_password = str(input("Enter the sys as sysdba password: "))
  sysdba_password2 = getpass.getpass(prompt='Enter the sys as sysdba password: ', stream=None)
  print(sysdba_password2)
  file_name_array = []
  if not tablespace_name or tablespace_name == "":
    print ('')
    print ('   ***** Must input a tablespace name!  Exiting script')
    exit()
  if not host_name or host_name == "":
    print ('')
    print ('   ***** Must input a host name!  Exiting script')
    exit()
  if not port_number or port_number == "":
    print ('')
    print ('   ***** Must input a port number!  Exiting script')
    exit()
  if not service_name or service_name == "":
    print ('')
    print ('   ***** Must input a service name!  Exiting script')
    exit()
  if not sysdba_password or sysdba_password == "":
    print ('')
    print ('   ***** Must input a sysdba password!  Exiting script')
    exit()
  confirmation_msg = '################################################################## \n'
  confirmation_msg = confirmation_msg + '###              Confirm Script Inputs:                        ### \n'
  if pdb_name:
     confirmation_msg = confirmation_msg + '###        PDB Being Encrypted: ' + pdb_name + ' \n'
  confirmation_msg = confirmation_msg + '### Tablespace Being Encrypted: ' + tablespace_name + ' \n'
  confirmation_msg = confirmation_msg + '###                  Host Name: ' + host_name + ' \n'
  confirmation_msg = confirmation_msg + '###                Port Number: ' + port_number + ' \n'
  confirmation_msg = confirmation_msg + '###               Service Name: ' + service_name + ' \n'
  confirmation_msg = confirmation_msg + '###                                                            ### \n'
  confirmation_msg = confirmation_msg + '################################################################## \n'

  print ('')
  print (confirmation_msg)
  print ('')
  confirmation_flg = str(input("Press Y/y to confirm, any other key to exit: ")).upper()
  if not confirmation_flg or confirmation_flg != "Y":
     exit()

  logpath = os.path.dirname(os.path.realpath(__file__)) + '/log'
  connection_string = host_name + ':' + port_number + '/' + service_name
  print('Connection String: ' + connection_string)
  if not os.path.isdir(logpath):
    print ('Log directory does not exist - creating directory: ' + logpath)
    try:
      os.mkdir(logpath)
    except OSError:
      print ("Creation of the directory %s failed" % logpath)
      sys.exit()
    else:
      print ("Successfully created the directory %s " % logpath)
  if not pdb_name:
    logging.basicConfig(filename='./log/non-cdb_' + tablespace_name + '_encryption_' + log_date + '.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
  else:
    logging.basicConfig(filename='./log/pdb_' + pdb_name + '_' + tablespace_name + '_encryption_' + log_date + '.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
  logging.info('confirmation_msg')
  logging.info('Executing ' + str(num_threads) + ' threads')
  con = cx_Oracle.connect(user="sys", password=sysdba_password,
                          dsn=connection_string,
                          mode = cx_Oracle.SYSDBA)
  cur = con.cursor()
  if not pdb_name:
    sql_text = 'select df.name from v$tablespace ts, v$datafile df \n'
    sql_text = sql_text + 'where ts.ts#=df.ts# and (ts.name not in (\'SYSTEM\',\'SYSAUX\') \n'
    sql_text = sql_text + 'and ts.name not in (select value from gv$parameter where name=\'undo_tablespace\')) \n'
    sql_text = sql_text + 'and ts.name not like \'UNDO%\' \n'
    sql_text = sql_text + 'and ts.name = \'' + tablespace_name + '\' order by df.file#'
  else:
    sql_text = 'select df.name from v$tablespace ts, v$datafile df, v$pdbs pdb \n'
    sql_text = sql_text + 'where ts.ts#=df.ts# and ts.con_id=df.con_id and (ts.name not in (\'SYSTEM\',\'SYSAUX\') \n'
    sql_text = sql_text + 'and ts.name not in (select value from gv$parameter where name=\'undo_tablespace\')) \n'
    sql_text = sql_text + 'and df.con_id=pdb.con_id and ts.name not like \'UNDO%\' \n'
    sql_text = sql_text + 'and pdb.name = \'' + pdb_name + '\' order by df.con_id, df.file# \n'
    sql_text = sql_text + 'and ts.name = \'' + tablespace_name + '\' order by df.con_id, df.file# \n'
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

  print(str(len(file_name_array)) + ' Datafiles to Encrypt')
  total_datafiles_remaining = len(file_name_array)
  logging.info(str(len(file_name_array)) + ' Datafiles to Encrypt')

  with PoolExecutor(max_workers=num_threads) as executor:
    for _ in executor.map(encrypt_proc, file_name_array):
      total_datafiles_remaining = total_datafiles_remaining - 1
      print('Datafiles remaining to encrypt: ' + str(total_datafiles_remaining))
      pass

  logging.info('Script completed')
  print('Script completed')
