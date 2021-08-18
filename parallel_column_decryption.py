#!/usr/bin/python3

import cx_Oracle, time, os, logging, random, sys
import multiprocessing as mp
import datetime
from concurrent.futures import ProcessPoolExecutor as PoolExecutor
import getpass
#import pdb; pdb.set_trace()

def encrypt_proc(args):
   table_owner_arg = args[0]
   table_name_arg = args[1]
   column_name_arg = args[2]
   begin_time = datetime.datetime.now()
   con = cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
   cur = con.cursor()
   if pdb_name:
     sql_text = 'alter session set container=' + pdb_name
     cur.execute(sql_text)
   sql_text = 'alter session set ddl_lock_timeout=600'
   cur.execute(sql_text)
   sql_text = 'alter table ' + table_owner_arg + '.' + table_name_arg + ' modify (' + column_name_arg + ' decrypt)'
   try:
     cur.execute(sql_text)
   except cx_Oracle.DatabaseError as e:
     error, = e.args
     if error.code == 28431:
       print('**ERROR on Table / Column: ' + table_owner_arg + '.' + table_name_arg + '/' + column_name_arg + ' ---  Error: ' + error.code + '  :  ' + error.message + '  :  ' + error.context)
       logging.info('**ERROR on Table / Column: ' + table_owner_arg + '.' + table_name_arg + '/' + column_name_arg + ' ---  Error: ' + error.code + '  :  ' + error.message + '  :  ' + error.context)
     else:
       print('**ERROR on Table / Column: ' + table_owner_arg + '.' + table_name_arg + '/' + column_name_arg + ' ---  Error: ' + error.code + '  :  ' + error.message + '  :  ' + error.context)
       logging.info('**ERROR on Table / Column: ' + table_owner_arg + '.' + table_name_arg + '/' + column_name_arg + ' ---  Error: ' + error.code + '  :  ' + error.message + '  :  ' + error.context)
   cur.close()
   con.close()
   total_time = datetime.datetime.now() - begin_time
   logging.info('Finished Execution of ' + sql_text)
   if total_time.seconds > 0:
     logging.info('    Completed Encryption in ' + str(total_time.seconds) + ' seconds')
   else:
     logging.info('    Completed Encryption in ' + str(total_time.microseconds / 1000) + ' milliseconds')
   return

if __name__ == '__main__':
  log_date = datetime.datetime.now().strftime("%m%d%Y%H%M")
  print('****** Ensure you have the correct environment sourced in BEFORE PROCEEDING ******')
  pdb_name = str(input("Enter PDB you wish to encrypt.  Press enter for non-cdb: ")).upper()
  schema_name = str(input("Enter the schema owner of the columns you wish to decrypt: ")).upper()
  num_threads = int(input("Enter the number of threads you want to execute: "))
  num_threads = min(num_threads, mp.cpu_count())
  host_name = str(input("Enter the host name where the database resides.  Default localhost: ") or "localhost").lower()
  port_number = str(input("Enter the port number if different than the default port of 1521: ") or "1521")
  service_name = str(input("Enter the service name to connect to: ")).lower()
  sysdba_password = getpass.getpass(prompt='Enter the sys as sysdba password: ', stream=None)
  file_name_array = []
  if not schema_name or schema_name == "":
    print ('')
    print ('   ***** Must input a schema name!  Exiting script')
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
  confirmation_msg = confirmation_msg + '###     Schema Being Decrypted: ' + schema_name + ' \n'
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
    logging.basicConfig(filename='./log/non-cdb_column_decryption_' + log_date + '.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
  else:
    logging.basicConfig(filename='./log/pdb_' + pdb_name + '_column_decryption_' + log_date + '.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
  logging.info('confirmation_msg')
  logging.info('Executing ' + str(num_threads) + ' threads')
  con = cx_Oracle.connect(user="sys", password=sysdba_password,
                          dsn=connection_string,
                          mode = cx_Oracle.SYSDBA)
  cur = con.cursor()
  if not pdb_name:
    sql_text = 'select owner,table_name,column_name from dba_encrypted_columns where owner = \'' + schema_name + '\' order by column_name'
  else:
    sql_text = 'select owner,table_name,column_name from cdb_encrypted_columns c, v$pdbs pdb \n'
    sql_text = sql_text + 'where c.owner = \'' + schema_name + '\'  and c.con_id=pdb.con_id \n'
    sql_text = sql_text + 'and pdb.name = \'' + pdb_name + '\' order by column_name'
  try:
    cur.execute(sql_text)
    encrypted_column_result = cur.fetchall()
    cur.close()
    con.close()
  except cx_Oracle.DatabaseError as e:
    error, = e.args
    print('**SQL ERROR: ' + file_name_arg + ' Error: ' + error.code + '  :  ' + error.message + '  :  ' + error.context)
    logging.info('**SQL ERROR: ' + file_name_arg + ' Error: ' + error.code + '  :  ' + error.message + '  :  ' + error.context)

  logging.info('Potiential columns to be decrypted:')
  for params in encrypted_column_result:
    logging.info('Table: ' + str(params[0] + '.' + params[1]) + ' Column: ' + str(params[2]))
    print('Decrypting Table: ' + str(params[0] + '.' + params[1]) + ' Column: ' + str(params[2]))

  print('Starting Parallel Worker Threads.....')
  logging.info('Starting Parallel Worker Threads.....')

  print(str(len(encrypted_column_result)) + ' Columns to Decrypt')
  total_columns_remaining = len(encrypted_column_result)
  logging.info(str(len(encrypted_column_result)) + ' Columns to Decrypt')

  with PoolExecutor(max_workers=num_threads) as executor:
    for _ in executor.map(encrypt_proc, encrypted_column_result):
      total_columns_remaining = total_columns_remaining - 1
      print('Columns remaining to decrypt: ' + str(total_columns_remaining))
      pass

  if pdb_name:
    logging.info('Script for ' + pdb_name + ' completed at ' + datetime.datetime.now())
    print('Script for ' + pdb_name + ' completed at ' + datetime.datetime.now())
  else:
    logging.info('Script for non_cdb completed at ' + datetime.datetime.now())
    print('Script for non_cdb completed at ' + datetime.datetime.now())