# ДАННЫЕ ПО ПЕРИОДИЧЕСКИМ ОТСЧЕТАМ ВРЕМЕНИ

import sys
import pandas as pd
import psycopg2
import os
import shutil
import datetime
import codecs

# Для реализации выхода по окончании через Enter:
try:
    from msvcrt import getch
except ImportError:
    import sys
    import tty, termios
    def getch():
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch

def stop(symbol, message):
    while True:
        print(message)
        if getch() == symbol:
            break

# для основной программы:
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

def sql_script(path_script, date_string): # Функция запуска sql-скрипта
    f = codecs.open( path_script, "r", "utf-8" )
    #f = open(file,'r')
    s = f.read()
    sql = s.replace('[date]', date_string)
    sql_coms = sql.replace('\n', ' ').split(';')[:-1]
    
    for sql_com in sql_coms:
        curs.execute(sql_com)
     
    f.close()
 
def ETL(table_name, path_source, path_destination):  # Находит csv-файлы в path_source, вставляет данные в таблицу table_name, cохраняет файлы в архив в path_destination 

    #Считываем дату последней загрузки из таблицы метаданных
  curs.execute(f"select max_update_dt from meta_all where table_name = '{table_name}'")
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

  # print(date_start_search)
  # str_date = input('Input date:')
  file_found = False
  print(f'Reading files from {str_date_start_search} ...')
  dir_list = os.listdir(path_source)
  files = []
  if dir_list:
      for j in dir_list:
          if j >= str_date_start_search: # Ищем новые папки (с даты последней загрузки минус 1 день)
              lstdir = os.listdir(path_source + j )
              # print('lstdir:', lstdir)
              for i in lstdir:
                  if i.find('0000_000.csv') > -1: 
                      file_found = True
                      files.append(path_source + j + '/0000_000.csv') # Формируем список путей к файлам
              if not files: 
                   print (f'folder {j} is empty')
                   # sys.exit()
          else: 
              print (f'there are only old files in {j}')
              # sys.exit()
  else: 
      print (f'files are not found in {path_source}')
      # sys.exit()
  files.sort() # обязательно, иначе может сначала загрузить новые, и более старые уже грузить не будет!
  print('Files for extracting:', files)


  # Перебираем все файлы в списке:
  total_rows_counter = 0 #сумма всех вставленных строк
  for file in files:
      print(f'= start ETL for {file} : =')

      # Считываем датафреймы из файлов:
      df = pd.read_csv(file, sep = ',')
      df = df.dropna() # чистим от NaN !
      df = df.reset_index(drop=True) #обновляем индексацию, чтобы не путались индескы удаленных dropna строк !
      df = df.astype(str)
      # print(df) 
      print('Readed')

      # Получаем заголовки колонок (кроме первого), 
      # выделяем из них названия столбцов в таблице БД (удалением префикса 'PLC_') и
      # формируем строки колонок БД для SQL команды insert into:
      df_columns = df.columns.values.tolist()
      df_columns.remove('Timestamp')
      
      # print('Columns: ', df_columns)
      columns_string = 'time, '
      # values_string = ''
      # values_string = "to_timestamp('{df['Timestamp'][i]}', 'YYYY-MM-DD HH24:MI:SS.MS' ) + interval '3 hours', "
      for i in df_columns: 
        columns_string += i.replace('PLC_', '', 1) + ', '
      columns_string = columns_string[:-2] # удаляем последнюю запятую
      

      # Фильтруем старые файлы:
      max_date = df['Timestamp'][len(df['Timestamp']) - 1] # Самая поздняя в файле метка времени
      print(f'max date in file: {max_date}')
      if max_date < date_from_meta_str: continue # Переходим к следущему файлу, если последняя строчка в нем датируется раньше последней загрузки 

      # ЗАГРУЗКА НОВЫХ ДАННЫХ
      print('Loading new data to DWH...')
      c = 0
      print('Starting from:', df['Timestamp'][0])
      for i in range (0,len(df['Timestamp'])):
          if df['Timestamp'][i] > date_from_meta_str: 
              # Формируем значения для values команды INSERT INTO:
              values_string = f"to_timestamp('{df['Timestamp'][i]}', 'YYYY-MM-DD HH24:MI:SS.MS' ) + interval '3 hours', "
              for column in df_columns: 
                values_string += f" '{df[column][i]}' ,"
              values_string = values_string[:-2]  
              # Проверяем соответствие csv и таблицы в БД:
              # SELECT EXISTS(SELECT column_name FROM 
              # cur.fetchall()

              # Всатвляем строки:
              curs.execute(f'''
                  insert into {table_name} 
              (    
                 {columns_string}
               ) 
              values (   
                 {values_string}
                       ) 
              on conflict do nothing
                          ''' )

              if c % 1000 == 0: print(df['Timestamp'][i], end = '|') #Выводим каждую 1000 метку времени (для наблюдения за процессом)
              c += 1

              # обновляем таблицу параметров:
              if table_name = 'nord_1d': 
                sql_script('C:/ETL/nord/update_scd2_of_parametrs.sql','')

      print(c,' rows inserted')
      total_rows_counter += c 
      
    
      # 4. ОБНОВЛЕНИЕ МЕТАДАННЫХ (скрипт meta.sql)
      print('Updating metadata...')
      curs.execute(f"""insert INTO meta_all ( table_name, max_update_dt ) 
      values ('{table_name}', coalesce( to_timestamp( '{max_date}', 'YYYY-MM-DD HH24:MI:SS.MS' ), to_date( '1900.01.01', 'YYYY.MM.DD' ))) 
      on conflict (table_name) do
          update set max_update_dt = to_timestamp('{max_date}', 'YYYY-MM-DD HH24:MI:SS.MS' )""")
      print('Updated')

      # Фиксируем изменения
      print('Commit')
      conn.commit()

      # Сохраняем файл в архив
      print('Saving file to archive...')
      folder = file[-21:-13] 
      folder_end = max_date[11:19].replace (':','_')  
      if not os.path.exists(path_destination + folder):
          shutil.copytree(path_source + folder, path_destination + folder)
      else: 
        if not os.path.exists(path_destination + folder + folder_end):
          shutil.copytree(path_source + folder, path_destination + folder + folder_end)
        else: 
          if not os.path.exists(path_destination + folder + '_ _'): 
            shutil.copytree(path_source + folder, path_destination + folder + '_ _')
          else: print(f"FILE {path_source + folder, path_destination + folder + '_ _'} IS NOT CREATED!")  
      print(f'= stop ETL for {file} : =')  
  print('Total rows inserted: ', total_rows_counter)


# ЦИКЛ ПРОГРАММЫ:
# Коннектимся с сервером
print('=== ETL start ===')
print('Connecting...')
conn = create_connection("kip", "kip", "kip", "192.168.100.223", "5432")
# conn = create_connection("kiplocal", "postgres", "111111", "localhost", "5432")
conn.autocommit = False
curs = conn.cursor()
print('Connected')

sources_list = ['Analog_data_1d','Analog_data_1s', 'Analog_data_1min', 'Analog_data_1h', 'alkali', 'alarm_data','Other']
table_list = ['nord_1d','nord_1s', 'nord_1min', 'nord_1h', 'nord_alkali', 'nord_alarm','nord_other']
# sources_list = ['Other']
# table_list = ['nord_other']
k=0
for i in sources_list:
  print(f'+++ start ETL for {i} +++' )
  src_pth = r'D:/Data Logging/Log Files/' + i + '/'
  dst_pth = r'C:/ETL/nord/Архив/' + i + '/'
  ETL(table_list[k], src_pth, dst_pth)
  print(f'+++ end ETL for {i} +++' )
  k += 1
 
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

# Закрываем окно только после нажатия Enter
stop(b'\r', 'Press Enter to exit')