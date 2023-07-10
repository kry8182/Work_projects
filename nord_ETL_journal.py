# Журнал

import sys
import pandas as pd
import psycopg2
import os
import shutil
# from datetime import datetime
import datetime



def sql_script(path_script, date_string): # Функция запуска sql-скрипта
    f = open(path_script,'r')
    s = f.read()
    sql = s.replace('[date]', date_string)
    sql_coms = sql.replace('\n', ' ').split(';')[:-1]
    for sql_com in sql_coms:
        curs.execute(sql_com)
    f.close()

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

# Коннектимся с сервером
print('=== ETL start ===')
print('Connecting...')
conn = create_connection("kip", "kip", "kip", "192.168.100.223", "5432")
conn.autocommit = False
curs = conn.cursor()
print('Connected')

#Считываем дату последней загрузки из таблицы метаданных
curs.execute("select max_update_dt from meta_all where table_name = 'nord_journal'")
date_from_meta = curs.fetchall()
if len(date_from_meta) > 0:
    t = date_from_meta[0][0] 
    # - pd.to_timedelta('3 hours') # В БД будем писать по местному времени, а в csv пишется по UTC.
    #t = date_from_meta[0][0]
    date_from_meta_str = t.strftime("%Y-%m-%d %H:%M:%S") 
else: 
    t = datetime.datetime(1900,1,1,0,0)
    date_from_meta_str = t.strftime("%Y-%m-%d %H:%M:%S")
print('max date in meta:', date_from_meta_str)
date_start_search = t - pd.to_timedelta('1 day')
str_date_start_search = date_start_search.strftime("%Y%m%d")

# Получение путей к файлам 

file_found = False
  # print(f'Reading files from {str_date_start_search} ...')
path_source = r'E:/'
dir_list = os.listdir(path_source)
print(dir_list)
files = []
if 'AlarmLog.csv' in dir_list: 
    file_found = True
else:print (f'files are not found in {path_source}')
if file_found:
  file = path_source + 'AlarmLog.csv'
  total_rows_counter = 0 #сумма всех вставленных строк

  print(f'= start ETL for {file} : =')

  # Считываем датафреймы из файлов:
  columns_str = ('time', 'name', 'Unnamed: 2', 'text','unnamed3','level','front','user')
  df = pd.read_csv(file, sep = ',',encoding='utf-8', header=None, names=columns_str)  
  #print(df)
  df = df.dropna(subset=['time']) # чистим от NaN !
  df = df.astype(str)
  df = df.reset_index(drop=True) #обновляем индексацию, чтобы не путались индескы удаленных dropna строк !
  # max_update_dt = datetime.strptime(df['time'][len(df['time']) - 1],'%-d/%-m/%Y %-I:%M')
  max_date_t = datetime.datetime.strptime(df['time'][0],'%m/%d/%Y %I:%M:%S %p')
  # print(max_date_t, type(max_date_t))

  
#   print(df) 
  print('Readed')
  
  print(f'max date in file: {max_date_t}')
  if max_date_t < t: exit() # Переходим к следущему файлу, если последняя строчка в нем датируется раньше последней загрузки   
  # 2. Загрузка новых данных
  print('Loading changes to DWH...')
  c = 0
  print('Starting from:', df['time'][len(df['time'])-1])
  for i in range (0,len(df['time'])):
      if datetime.datetime.strptime(df['time'][i],'%m/%d/%Y %I:%M:%S %p') > t: 
        if df['front'][i] == 'Alarm Raised': 
            front = 1
        elif df['front'][i] == 'Alarm Cleared' or df['front'][i] == 'Alarm Cleared (acknowledged)' : 
            front = 0
        else: front = 2
        curs.execute(f'''
          insert into nord_journal 
      ( 
         time   ,
         name,
         level,
         front
       ) 
      values (
      to_timestamp('{df['time'][i]}', 'MM/DD/YYYY HH12:MI:SS pm' ) + interval '3 hours',
         '{df['name'][i]}',                                
         '{df['level'][i]}',
         '{front}'
               ) 
      on conflict do nothing
                  ''' ) 
        c += 1
        if front == 1:
          curs.execute(f'''
          insert into nord_journal 
        (time, name, level, front)
        values (
        to_timestamp('{df['time'][i]}', 'MM/DD/YYYY HH12:MI:SS pm' ) + interval '3 hours' - interval '100 milliseconds',
         '{df['name'][i]}',                                
         '{df['level'][i]}',
         0
               )
        on conflict do nothing
                  ''' ) 
          c += 1
        elif front == 0:
          curs.execute(f'''
          insert into nord_journal 
        (time, name, level, front)
        values (
        to_timestamp('{df['time'][i]}', 'MM/DD/YYYY HH12:MI:SS pm' ) + interval '3 hours' - interval '100 milliseconds',
         '{df['name'][i]}',                                
         '{df['level'][i]}',
         1
               )
        on conflict do nothing  
                  ''' ) 
          c += 1
        if c % 1000 == 0: print(df['time'][i], end = '|')
        
  print(c,' rows inserted')
  
  
  
  # 4. ОБНОВЛЕНИЕ МЕТАДАННЫХ (скрипт meta.sql)
  if max_date_t > t:
      print('Updating metadata...')
      curs.execute(f"""insert INTO meta_all ( table_name, max_update_dt ) 
      values ('nord_journal', coalesce(to_timestamp( '{max_date_t}', 'YYYY-MM-DD HH24:MI:SS.MS' ), to_date( '1900.01.01', 'YYYY.MM.DD' ))) 
      on conflict (table_name) do
          update set max_update_dt = to_timestamp( '{max_date_t}', 'YYYY-MM-DD HH24:MI:SS.MS' )""")
      print('Updated') 
  else: print('Metadata is not updated (max date in file <= date from meta)')    
  # Фиксируем изменения
  print('Commit')
  conn.commit()
  curs.close()
  conn.close()
  print('Connect closed')

  # # Сохраняем файл в архив
  folder = max_date_t.strftime('%m-%d-%Y %I:%M:%S %p')
  folder = folder[:19].replace ('-','')
  folder = folder.replace (':','_')  
  print(f'Saving file to archive (folder {folder}) ...')

  if not os.path.exists(r'C:/ETL/nord/Архив/journals/' + folder):
      os.makedirs(r'C:/ETL/nord/Архив/journals/' + folder)  
      shutil.copyfile(r'E:/AlarmLog.csv',r'C:/ETL/nord/Архив/journals/' + folder + r'/AlarmLog.csv')  
  else: 
      print('Folder with this name already exists, copying has been canceled !')    
# print(f'= stop ETL for {file} : =')    
# #2. ПОСТРОЕНИЕ ОТЧЕТА 
# print('Report creating...')
# sql_script('', date)
# print('Created')

# # Фиксируем изменения в таблице отчетов
# print('Commit')
# conn.commit()

# Закрываем коннект
curs.close()
conn.close()
print('Connect closed')

print('=== ETL complete ====')