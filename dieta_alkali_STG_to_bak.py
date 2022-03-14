#!/usr/bin/python
# dieta to _bak через STG

import sys
import pandas
import psycopg2
import os
import datetime


def sql_script(path_script, date_string): # Функция запуска sql-скрипта
    f = open(path_script,'r')
    s = f.read()
    sql = s.replace('[date]', date_string)
    sql_coms = sql.replace('\n', ' ').split(';')[:-1]
    for sql_com in sql_coms:
        curs.execute(sql_com)
    f.close()

def backup(path, dir, end): # Функция бэкапа в папку dir с добавлению к имени файла окочания end
    os.rename(path, path[0: path.rfind(r'/') + 1] + dir + r'/' + path[ path.rfind(r'/') + 1:-1] + path[-1]  + end)

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

# def execute_query(connection, query):
#     connection.autocommit = False
#     cursor = connection.cursor()
#     try:
#         cursor.execute(query)
#         print("Query executed successfully")
#     except OperationalError as e:
#         print(f"The error '{e}' occurred")

# Коннектимся с сервером
print('=== ETL start ===')
print('Connecting...')
conn = create_connection("kip", "kip", "kip", "192.168.100.223", "5432")
conn.autocommit = False
curs = conn.cursor()
print('Connected')

#Очистка STG (скрипт truncate.sql)
print('Truncating STG-tables...')
# sql_script('/home/de1m/krlv/sql_scripts/truncate.sql','')
curs.execute("truncate table dieta_alkali_wash_stg")
print('Truncated')

# Получение путей к файлам и даты из имени файла
print('Reading files...')
path_alk_bak = ''
lstdir = os.listdir(r"C:/ETL/dieta/dieta_alkali_bak")
for i in lstdir:
    if i.find('Alkali_bak') > -1: 
        path_alk_bak = i
    # elif i.find('terminals_') > -1: path_term = i
    # elif i.find('transactions_') > -1: path_trans = i
# date_trans = path_trans[-12:-4]
# date_blk = path_blk[-13:-5]
# date_term = path_term[-13:-5]
if path_alk_bak > '': print(path_alk_bak)
else: 
     print ('files not found')
     sys.exit()

# Считываем датафреймы из файлов

# df_blk = pandas.read_excel( path_blk, sheet_name='blacklist', header=0, index_col=None )
# df_term = pandas.read_excel( path_term, sheet_name='terminals', header=0, index_col=None )
df_alk_bak = pandas.read_csv( 'C:/ETL/dieta/dieta_alkali_bak/' + path_alk_bak, sep = ',', usecols=['Group1', 'WR00006'] )
# df_alk_bak
df_alk_bak = df_alk_bak.astype(str)

# df_term = df_term.astype(str)
# df_trans = df_trans.astype(str)
# for i in range(0,len(df_alk_bak['Unnamed: 2'])):
#   df_alk_bak['Unnamed: 2'][i]= df_alk_bak['Unnamed: 2'][i].replace('nan','0')
print('Readed')
max_date = df_alk_bak['Group1'][len(df_alk_bak['Group1']) - 1]
print(f'max date in file: {max_date}')



#Считываем дату последней загрузки из таблицы метаданных
curs.execute("select max_update_dt from meta_all where table_name = 'dieta_alkali_bak'")
date_from_meta = curs.fetchall()
if len(date_from_meta) > 0:
    t = date_from_meta[0][0]
    date_from_meta_str = t.strftime("%Y/%m/%d %H:%M:%S")
else: 
    t = datetime.datetime(1900,1,1,0,0)
    date_from_meta_str = t.strftime("%Y/%m/%d %H:%M:%S")
print('max date in meta:', date_from_meta_str)

# 1.ЗАПОЛНЕНИЕ STG
# Вставляем датафреймы в таблицы
print('Loading file to STG...')
# print( df_alk_bak.values.tolist())
curs.executemany( "insert into dieta_alkali_wash_stg( time, conc) values ( to_timestamp( %s, 'YYYY/MM/DD HH24:MI' ), %s) ", df_alk_bak.values.tolist() )
print(f'Loaded {curs.rowcount} rows to STG')
# c = 0
# for i in range (0, len(df_alk_bak['Group1'])):
#     curs.execute(f"insert into dieta_alkali_wash_stg ( time, conc ) values (to_timestamp( '{df_alk_bak['Group1'][i]}', 'YYYY/MM/DD HH24:MI' ), '{df_alk_bak['WR00006'][i]}') ")
#     c =+ c
# print(c,' rows inserted')
# print('Loaded')

# # Заполняем остальные STG (скрипт extract.sql)
# print('Extract changes to STG...')
# sql_script('/home/de1m/krlv/sql_scripts/extract.sql','')
# print('Extracted')

# 2. ВЫДЕЛЕНИЕ ВСТАВОК И ИЗМЕНЕНИЙ (скрипт transform_load.sql)
print('Loading changes to DWH...')
# c = 0
# for i in range (0, len(df_alk_bak['Group1'])):
#     if df_alk_bak['Group1'][i] > date_from_meta_str: 
#         curs.execute(f"insert into dieta_alkali_bak ( time, conc ) values (to_timestamp( '{df_alk_bak['Group1'][i]}', 'YYYY/MM/DD HH24:MI' ), '{df_alk_bak['WR00006'][i]}') ")
#         c =+ c
# print(c,' rows inserted')
# sql_script('/home/de1m/krlv/sql_scripts/transform_load.sql', date_term)
curs.execute(""" insert into dieta_alkali_bak ( time, conc ) 
select
    stg.time,
    stg.conc
from  dieta_alkali_wash_stg stg
left join dieta_alkali_bak tgt
on ( stg.time = tgt.time )
where tgt.time is null """)
print(f'Loaded {curs.rowcount} rows to DWH')


# # 3.ОБРАБОТКА УДАЛЕНИЙ (скрипт deleted.sql)
# sql_script('/home/de1m/krlv/sql_scripts/deleted.sql', date_term)
# print('Loaded')

# 4. ОБНОВЛЕНИЕ МЕТАДАННЫХ (скрипт meta.sql)
print('Updating metadata...')
# sql_script('/home/de1m/krlv/sql_scripts/meta.sql', date_blk)
curs.execute("select max(time) from dieta_alkali_bak")
max_date_from_DWH = curs.fetchall();
t = max_date_from_DWH[0][0]
max_date_from_DWH_str = t.strftime("%Y/%m/%d %H:%M:%S")
print(f'New max_date_to_meta:{max_date_from_DWH_str}')
curs.execute(f"""insert INTO meta_all ( table_name, max_update_dt ) 
values ('dieta_alkali_bak', coalesce( to_timestamp( '{max_date_from_DWH_str}', 'YYYY/MM/DD HH24:MI:SS' ), to_date( '1900.01.01', 'YYYY.MM.DD' ))) 
on conflict (table_name) do
    update set max_update_dt = to_timestamp('{max_date_from_DWH_str}', 'YYYY/MM/DD HH24:MI:SS' )""")
print('Updated')

# Фиксируем изменения
print('Commit')
conn.commit()

# #2. ПОСТРОЕНИЕ ОТЧЕТА НА ДАТУ ЗАГРУЖЕННОГО ФАЙЛА ТРАНЗАКЦИЙ
# print('Report creating...')
# sql_script('/home/de1m/krlv/sql_scripts/report.sql', date_trans)
# print('Created')

# # Фиксируем изменения в таблице отчетов
# print('Commit')
# conn.commit()

# Закрываем коннект
curs.close()
conn.close()
print('Connect closed')

# Отправляем использованные файлы в архив 
backup('C:/ETL/dieta/' + path_alk_bak, 'arhive', '.backup')
# backup(path_trans, 'arhive', '.backup')
# backup(path_blk, 'arhive', '.backup')
print('Backup done')
print('=== ETL complete ====')