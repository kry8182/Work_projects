#!/usr/bin/python

import pandas
import psycopg2
import os

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
curs.execute("truncate table tvorog_alkali_wash_stg")
print('Truncated')

# Получение путей к файлам и даты из имени файла
print('Reading files...')
lstdir = os.listdir(r"C:/ETL/tvorog/Tvorog_alkali_bak")
for i in lstdir:
	if i.find('Tvorog_alkali_bak') > -1: 
		path_tvg_alk_bak = i
	# elif i.find('terminals_') > -1: path_term = i
	# elif i.find('transactions_') > -1: path_trans = i
# date_trans = path_trans[-12:-4]
# date_blk = path_blk[-13:-5]
# date_term = path_term[-13:-5]


# Считываем датафреймы из файлов

# df_blk = pandas.read_excel( path_blk, sheet_name='blacklist', header=0, index_col=None )
# df_term = pandas.read_excel( path_term, sheet_name='terminals', header=0, index_col=None )
df_tvg_alk_bak = pandas.read_csv( 'C:/ETL/tvorog/Tvorog_alkali_bak/' + path_tvg_alk_bak, sep = ',' )
# df_tvg_alk_bak
df_tvg_alk_bak = df_tvg_alk_bak.astype(str)

# df_term = df_term.astype(str)
# df_trans = df_trans.astype(str)
for i in range(0,len(df_tvg_alk_bak['Unnamed: 2'])):
	df_tvg_alk_bak['Unnamed: 2'][i]= df_tvg_alk_bak['Unnamed: 2'][i].replace('nan','0')
print('Readed')
max_date = df_tvg_alk_bak['Group1'][len(df_tvg_alk_bak['Group1']) - 1]
print(max_date)


# 1.ЗАПОЛНЕНИЕ STG
# Вставляем датафреймы в таблицы
print('Loading files to STG...')
# print( df_tvg_alk_bak.values.tolist())
curs.executemany( "insert into tvorog_alkali_wash_stg( time, conc, number_wash ) values ( to_timestamp( %s, 'YYYY/MM/DD HH24:MI' ), %s, %s) ", df_tvg_alk_bak.values.tolist() )
# curs.executemany( "insert into meta_all( table_name ) values ( %s )", ('test1','test2') )
# curs.executemany( "insert into de1m.krlv_stg_terminals( terminal_id, terminal_type, terminal_city, terminal_address ) values ( ?, ?, ?, ? )", df_term.values.tolist() )
# curs.executemany( "insert into de1m.krlv_stg_transactions( trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal  ) values ( ?, to_date( ?, 'YYYY-MM-DD HH24:MI:SS' ), cast( ? as decimal(12,2)), ?, ?, ?, ? )", df_trans.values.tolist() )
print('Loaded')

# # Заполняем остальные STG (скрипт extract.sql)
# print('Extract changes to STG...')
# sql_script('/home/de1m/krlv/sql_scripts/extract.sql','')
# print('Extracted')

# 2. ВЫДЕЛЕНИЕ ВСТАВОК И ИЗМЕНЕНИЙ (скрипт transform_load.sql)
print('Loading changes to DWH...')
# sql_script('/home/de1m/krlv/sql_scripts/transform_load.sql', date_term)
curs.execute(""" insert into tvorog_alkali_wash ( time, conc, number_wash )
select  time, cast(conc as decimal(10,2))/100 , number_wash from tvorog_alkali_wash_stg
where time > coalesce( ( 
    select max_update_dt
    from meta_all
    where table_name = 'tvorog_alkali_wash'
), to_date( '1800.01.01', 'YYYY.MM.DD' ))
order by time """)


# # 3.ОБРАБОТКА УДАЛЕНИЙ (скрипт deleted.sql)
# sql_script('/home/de1m/krlv/sql_scripts/deleted.sql', date_term)
# print('Loaded')

# 4. ОБНОВЛЕНИЕ МЕТАДАННЫХ (скрипт meta.sql)
print('Updating metadata...')
# sql_script('/home/de1m/krlv/sql_scripts/meta.sql', date_blk)
curs.execute(f"""insert INTO meta_all ( table_name, max_update_dt ) 
values ('tvorog_alkali_wash', coalesce( to_timestamp( '{max_date}', 'YYYY/MM/DD HH24:MI:SS' ), to_date( '1900.01.01', 'YYYY.MM.DD' ))) 
on conflict (table_name) do
    update set max_update_dt = to_timestamp('{max_date}', 'YYYY/MM/DD HH24:MI:SS' )""")
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
backup('C:/ETL/tvorog/Tvorog_alkali_bak/' + path_tvg_alk_bak, 'arhive', '.backup')
# backup(path_trans, 'arhive', '.backup')
# backup(path_blk, 'arhive', '.backup')
print('Backup done')
print('=== ETL complete ====')