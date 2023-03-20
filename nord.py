# pgadimn parol = 111111
import sys
import pandas as pd
import psycopg2
import os
import shutil
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
curs.execute("select max_update_dt from meta_all where table_name = 'nord_1s'")
date_from_meta = curs.fetchall()
if len(date_from_meta) > 0:
    t = date_from_meta[0][0] - pd.to_timedelta('3 hours') # В БД будем писать по местному времени, а в csv пишется по UTC.
    #t = date_from_meta[0][0]
    date_from_meta_str = t.strftime("%Y-%m-%d %H:%M:%S") 
else: 
    t = datetime.datetime(1900,1,1,0,0)
    date_from_meta_str = t.strftime("%Y-%m-%d %H:%M:%S")
print('max date in meta:', date_from_meta_str)
date_start_search = t - pd.to_timedelta('1 day')
str_date_start_search = date_start_search.strftime("%Y%m%d")

# Получение путей к файлам 

# date_start_search = date_ed.replace('/','')[0:9]
# print(date_start_search)
# str_date = input('Input date:')
print(f'Reading files from {str_date_start_search} ...')

# print(os.listdir(r"C:/ETL/nord/Analog_data_1s/"))
Analog_data_1s_list = os.listdir(r"D:/Data Logging/Log Files/Analog_data_1s/")
files_analog_1s = []
if Analog_data_1s_list:
    for j in Analog_data_1s_list:
        if j >= str_date_start_search: # Ищем новые папки (с даты последней загрузки минус 1 день)
            lstdir = os.listdir(r"D:/Data Logging/Log Files/Analog_data_1s/" + j )
            # print('lstdir:', lstdir)
            for i in lstdir:
                if i.find('0000_000.csv') > -1: 
                    files_analog_1s.append(r"D:/Data Logging/Log Files/Analog_data_1s/" + j + '/0000_000.csv') # Формируем список путей к файлам
            if not files_analog_1s: 
                 print ('files not found')
                 sys.exit()
        else: 
            print ('there are only old files')
            sys.exit()
else: 
    print ('files not found')
    sys.exit()
print('Analog_1s:', files_analog_1s)


# Перебираем все файлы в списке:
for file in files_analog_1s:
    print(f'= start ETL for {file} : =')
    # Считываем датафреймы из файлов
    df_1s = pd.read_csv(file, sep = ',')
    df_1s = df_1s.dropna() # чистим от NaN !
    df_1s = df_1s.reset_index(drop=True) #обнавляем индексацию, чтобы не путались индескы удаленных dropna строк !
    df_1s = df_1s.astype(str)
    # print(df_1s) 
    print('Readed')
    max_date = df_1s['Timestamp'][len(df_1s['Timestamp']) - 1]
    print(f'max date in file: {max_date}')
    if max_date < date_from_meta_str: continue # Переходим к следущему файлу, если последняя строчка в нем датируется раньше последней загрузки 

    # 2. Загрузка новых данных
    print('Loading changes to DWH...')
    c = 0
    print('Starting from:', df_1s['Timestamp'][0])
    for i in range (0,len(df_1s['Timestamp'])):
        if df_1s['Timestamp'][i] > date_from_meta_str: 
            curs.execute(f'''
                insert into nord_1s 
            ( 
               time   ,
               Temperature_CIP  ,  
               Temperature_na_vozvrate  ,  
               Alarm_bit    ,  
               Pressure_CIP ,  
               Potok    ,  
               Protok_CIP   ,  
               Temperature_CIP_2k   ,  
               Temperature_na_vozvrate_2k   ,  
               Pressure_CIP_2k  ,
               Alarm_bit_2k ,  
               Protok_CIP_2k    ,  
               Potok_2k,
               Show_0,
               Show_0_2k
             ) 
            values (
            to_timestamp('{df_1s['Timestamp'][i]}', 'YYYY-MM-DD HH24:MI:SS.MS' ) + interval '3 hours',
               '{df_1s['PLC_Temperature_CIP'][i]}',                                
               '{df_1s['PLC_Temperature_na_vozvrate'][i]}',
               '{df_1s['PLC_Alarm_bit'][i]}',
               '{df_1s['PLC_Pressure_CIP'][i]}',
               '{df_1s['PLC_Potok'][i]}',
               '{df_1s['PLC_Protok_CIP'][i]}',
               '{df_1s['PLC_Temperature_CIP_2k'][i]}',
               '{df_1s['PLC_Temperature_na_vozvrate_2k'][i]}',
               '{df_1s['PLC_Pressure_CIP_2k'][i]}',
               '{df_1s['PLC_Alarm_bit_2k'][i]}',
               '{df_1s['PLC_Protok_CIP_2k'][i]}',
               '{df_1s['PLC_Potok_2k'][i]}',
               '{df_1s['PLC_Show_0'][i]}',
               '{df_1s['PLC_Show_0_2k'][i]}'
               
                     ) 
            on conflict do nothing
                        ''' )

            if c % 1000 == 0: print(df_1s['Timestamp'][i], end = '|')
            c += 1
    print(c,' rows inserted')
    
  
    # 4. ОБНОВЛЕНИЕ МЕТАДАННЫХ (скрипт meta.sql)
    print('Updating metadata...')
    curs.execute(f"""insert INTO meta_all ( table_name, max_update_dt ) 
    values ('nord_1s', coalesce( to_timestamp( '{max_date}', 'YYYY-MM-DD HH24:MI:SS.MS' ), to_date( '1900.01.01', 'YYYY.MM.DD' ))) 
    on conflict (table_name) do
        update set max_update_dt = to_timestamp('{max_date}', 'YYYY-MM-DD HH24:MI:SS.MS' )""")
    print('Updated')

    # Фиксируем изменения
    print('Commit')
    conn.commit()

    # Сохраняем файл в архив
    print('Saving file to archive...')
    folder = file[41:-13]
    if not os.path.exists('C:/ETL/nord/Архив/Analog_data_1s/' + folder):
        shutil.copytree('D:/Data Logging/Log Files/Analog_data_1s/' + folder,'C:/ETL/nord/Архив/Analog_data_1s/' + folder)
    else: 
        shutil.copytree('D:/Data Logging/Log Files/Analog_data_1s/' + folder,'C:/ETL/nord/Архив/Analog_data_1s/' + folder + '_')
    print(f'= stop ETL for {file} : =')  

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

# # Отправляем использованные файлы в архив 

# backup('C:/ETL/nord/Analog_data_1s/' + path_analog_1s, 'arhive', '')
# # backup(path_trans, 'arhive', '.backup')
# # backup(path_blk, 'arhive', '.backup')
# print('Backup done')
print('=== ETL complete ====')