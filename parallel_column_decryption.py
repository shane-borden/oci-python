#!/usr/bin/python

import cx_Oracle, time, os, logging, random
import multiprocessing as mp
import datetime
from concurrent.futures import ProcessPoolExecutor as PoolExecutor
#import pdb; pdb.set_trace()

def encrypt_proc(args):
   table_owner_arg = args[0]
   table_name_arg = args[1]
   column_name_arg = args[2]
   begin_time = datetime.datetime.now()
   con = cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
   cur = con.cursor()
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
  pdb_name = str(raw_input("Enter PDB you wish to encrypt or Cntrl-C to exit: "))
  pdb_name = pdb_name.upper()
  num_threads = int(raw_input("Enter the number of threads you want to execute: "))
  num_threads = min(num_threads, mp.cpu_count())
  logging.basicConfig(filename='./log/' + pdb_name + '_column_decryption_' + log_date + '.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
  logging.info('Script for ' + pdb_name + ' began')
  logging.info('Executing ' + str(num_threads) + ' threads')
  con = cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
  cur = con.cursor()
  sql_text = 'alter session set container=' + pdb_name
  cur.execute(sql_text)
  sql_text = 'select owner,table_name,column_name from dba_encrypted_columns where (owner = \'SYSADM\' or owner like \'BI%\') order by column_name'
  cur.execute(sql_text)
  encrypted_column_result = cur.fetchall()
  cur.close()
  con.close()

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

  logging.info('Script for ' + pdb_name + ' completed')
  print('Script for ' + pdb_name + ' completed')