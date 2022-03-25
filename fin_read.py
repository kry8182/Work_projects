#!/usr/bin/python

import sys
import pandas
import psycopg2
import os
import datetime



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
conn = create_connection("kiplocal", "postgres", "111111", "localhost", "5432")
conn.autocommit = False
curs = conn.cursor()
print('Connected')

# #Очистка STG (скрипт truncate.sql)
# print('Truncating STG-tables...')
# # sql_script('/home/de1m/krlv/sql_scripts/truncate.sql','')
# curs.execute("truncate table fin_stg")
# print('Truncated')

# Получение путей к файлам и даты из имени файла
print('Reading files...')
path = ''
lstdir = os.listdir(r"C:/ETL/fin")
for i in lstdir:
    if i.find('.xls') > -1: 
        path = i
    # elif i.find('terminals_') > -1: path_term = i
    # elif i.find('transactions_') > -1: path_trans = i
# date_trans = path_trans[-12:-4]
# date_blk = path_blk[-13:-5]
# date_term = path_term[-13:-5]
if path > '': print(path)
else: 
     print ('files not found')
     sys.exit()

# Считываем датафреймы из файлов
# 'Январь2021'
month_date = input()
# month_date = 'Январь2021'
m_date = ''
if month_date.find('Январь') > -1: m_date = month_date.replace('Январь','01/01/')
elif month_date.find('Февраль') > -1: m_date = month_date.replace('Февраль','01/02/')
elif month_date.find('Март') > -1: m_date = month_date.replace('Март','01/03/')
elif month_date.find('Апрель') > -1: m_date = month_date.replace('Апрель','01/04/')
elif month_date.find('Май') > -1: m_date = month_date.replace('Май','01/05/')
elif month_date.find('Июнь') > -1: m_date = month_date.replace('Июнь','01/06/')
elif month_date.find('Июль') > -1: m_date = month_date.replace('Июль','01/07/')
elif month_date.find('Август') > -1: m_date = month_date.replace('Август','01/08/')
elif month_date.find('Сентябрь') > -1: m_date = month_date.replace('Сентябрь','01/09/')
elif month_date.find('Октябрь') > -1: m_date = month_date.replace('Октябрь','01/10/')
elif month_date.find('Ноябрь') > -1: m_date = month_date.replace('Ноябрь','01/11/')
elif month_date.find('Декабрь') > -1: m_date = month_date.replace('Декабрь','01/12/')


df_fin = pandas.read_excel('C:/ETL/fin/' + path, sheet_name=month_date, header=0, index_col=None, usecols=['Unnamed: 1', 'Unnamed: 15'] )
# , usecols=['Unnamed: 1', 'Unnamed: 16']
# df_term = pandas.read_excel( path_term, sheet_name='terminals', header=0, index_col=None )

# df_fin = df_fin.astype(str)
df_fin.fillna("0", inplace = True)
df_fin = df_fin.astype(int)
print(df_fin)

# df_term = df_term.astype(str)
# df_trans = df_trans.astype(str)
# for i in range(0,len(df_fin['Unnamed: 2'])):
#   df_fin['Unnamed: 2'][i]= df_fin['Unnamed: 2'][i].replace('nan','0')
print('Readed')
# max_date = df_fin['Unnamed: 1'][len(df_fin['Unnamed: 1']) - 1]
# print(f'max date in file: {max_date}')
max_date = m_date


#Считываем дату последней загрузки из таблицы метаданных
curs.execute("select max_update_dt from meta_all where table_name = 'fin'")
date_from_meta = curs.fetchall()
if len(date_from_meta) > 0:
    t = date_from_meta[0][0]
    date_from_meta_str = t.strftime("%Y/%m/%d %H:%M:%S")
else: 
    t = datetime.datetime(1900,1,1,0,0)
    date_from_meta_str = t.strftime("%Y/%m/%d %H:%M:%S")
print('max date in meta:', date_from_meta_str)

# # 1.ЗАПОЛНЕНИЕ STG
# # Вставляем датафреймы в таблицы
# print('Loading files to STG...')
# # print( df_fin.values.tolist())
# curs.executemany( "insert into fin_stg( time, conc, number_wash ) values ( to_timestamp( %s, 'YYYY/MM/DD HH24:MI' ), %s, %s) ", df_fin.values.tolist() )
# # curs.executemany( "insert into meta_all( table_name ) values ( %s )", ('test1','test2') )
# # curs.executemany( "insert into de1m.krlv_stg_terminals( terminal_id, terminal_type, terminal_city, terminal_address ) values ( ?, ?, ?, ? )", df_term.values.tolist() )
# # curs.executemany( "insert into de1m.krlv_stg_transactions( trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal  ) values ( ?, to_date( ?, 'YYYY-MM-DD HH24:MI:SS' ), cast( ? as decimal(12,2)), ?, ?, ?, ? )", df_trans.values.tolist() )
# print('Loaded')

# # Заполняем остальные STG (скрипт extract.sql)
# print('Extract changes to STG...')
# sql_script('/home/de1m/krlv/sql_scripts/extract.sql','')
# print('Extracted')

# 2. ВЫДЕЛЕНИЕ ВСТАВОК И ИЗМЕНЕНИЙ (скрипт transform_load.sql)
print('Loading changes to DWH...')
c = 0
curs.execute(f''' insert into fin 
    ( m_date,
    lise,
    x,
    topl,
    meg,
    meg2,
    bil,
    internet,
    gkh,
    prod,
    eg,
    zapas,
    apteka,
    podarki,
    ugoshen,
    hoztovar,
    neobhodimoe,
    rashodniki,
    obrazovan,
    kats,
    detsk_kapital,
    detsk_tekush,
    detsk_apt,
    detsk_med,
    odegda_obuv,
    kanctov,
    rash_printera,
    profilakt,
    medicine,
    modernization,
    proch,
    T3,
    matiz,
    shtrafs,
    pogertv,
    remonts,
    ubytki,
    selhoz,
    PSTGU,
    transport,
    sport,
    hobbi,
    dosug,
    kvart,
    nalogi,
    knigi,
    revival,
    stroyka,
    instrument,
    itogo,
    dostavka,
    vdolg,
    otpusk,
    largus,
    zp,
    derev_infrastructur,
    obsl_doma,
    obsheavto,
    kolhoz,
    avtodor,
    vsevologsk_infrastructur,
    posobia,
    liza_in,
    lesha_in,
    x_cpsh,
    auto_comp,
    rent,
    podarki_in,
    bonus,
    procent,
    vozvrat,
    itogo_in 
    ) 
values (timestamp'{m_date}',
        '{df_fin['Unnamed: 1'][1]}','{df_fin['Unnamed: 1'][2]}','{df_fin['Unnamed: 1'][3]}','{df_fin['Unnamed: 1'][4]}','{df_fin['Unnamed: 1'][5]}',
        '{df_fin['Unnamed: 1'][6]}','{df_fin['Unnamed: 1'][7]}','{df_fin['Unnamed: 1'][8]}','{df_fin['Unnamed: 1'][9]}','{df_fin['Unnamed: 1'][10]}',
        '{df_fin['Unnamed: 1'][11]}','{df_fin['Unnamed: 1'][12]}','{df_fin['Unnamed: 1'][13]}','{df_fin['Unnamed: 1'][14]}','{df_fin['Unnamed: 1'][15]}',
        '{df_fin['Unnamed: 1'][16]}','{df_fin['Unnamed: 1'][17]}','{df_fin['Unnamed: 1'][18]}','{df_fin['Unnamed: 1'][19]}','{df_fin['Unnamed: 1'][20]}',
        '{df_fin['Unnamed: 1'][21]}','{df_fin['Unnamed: 1'][22]}','{df_fin['Unnamed: 1'][23]}','{df_fin['Unnamed: 1'][24]}','{df_fin['Unnamed: 1'][25]}',
        '{df_fin['Unnamed: 1'][26]}','{df_fin['Unnamed: 1'][27]}','{df_fin['Unnamed: 1'][28]}','{df_fin['Unnamed: 1'][29]}','{df_fin['Unnamed: 1'][30]}',
        '{df_fin['Unnamed: 1'][31]}','{df_fin['Unnamed: 1'][32]}','{df_fin['Unnamed: 1'][33]}','{df_fin['Unnamed: 1'][34]}','{df_fin['Unnamed: 1'][35]}',
        '{df_fin['Unnamed: 1'][36]}','{df_fin['Unnamed: 1'][37]}','{df_fin['Unnamed: 1'][38]}','{df_fin['Unnamed: 1'][39]}','{df_fin['Unnamed: 1'][40]}',
        '{df_fin['Unnamed: 1'][41]}','{df_fin['Unnamed: 1'][42]}','{df_fin['Unnamed: 1'][43]}','{df_fin['Unnamed: 1'][44]}','{df_fin['Unnamed: 1'][45]}',
        '{df_fin['Unnamed: 1'][46]}','{df_fin['Unnamed: 1'][47]}','{df_fin['Unnamed: 1'][48]}','{df_fin['Unnamed: 1'][49]}','{df_fin['Unnamed: 1'][50]}',
        '{df_fin['Unnamed: 1'][51]}','{df_fin['Unnamed: 1'][52]}','{df_fin['Unnamed: 1'][53]}','{df_fin['Unnamed: 1'][54]}','{df_fin['Unnamed: 1'][55]}',
        '{df_fin['Unnamed: 1'][56]}','{df_fin['Unnamed: 1'][57]}','{df_fin['Unnamed: 1'][58]}','{df_fin['Unnamed: 1'][59]}','{df_fin['Unnamed: 1'][60]}',
        '{df_fin['Unnamed: 15'][0]}',
        '{df_fin['Unnamed: 15'][1]}','{df_fin['Unnamed: 15'][2]}','{df_fin['Unnamed: 15'][3]}','{df_fin['Unnamed: 15'][4]}','{df_fin['Unnamed: 15'][5]}',
        '{df_fin['Unnamed: 15'][6]}','{df_fin['Unnamed: 15'][7]}','{df_fin['Unnamed: 15'][8]}','{df_fin['Unnamed: 15'][9]}','{df_fin['Unnamed: 15'][10]}'
        )
 ''')
c =+ c
print(c,' rows inserted')
# sql_script('/home/de1m/krlv/sql_scripts/transform_load.sql', date_term)
# curs.execute(""" insert into fin ( time, conc, number_wash )
# select  time, cast(conc as decimal(10,2))/100 , number_wash from fin_stg
# where time > coalesce( ( 
#     select max_update_dt
#     from meta_all
#     where table_name = 'fin'
# ), to_date( '1800.01.01', 'YYYY.MM.DD' ))
# order by time """)


# # 3.ОБРАБОТКА УДАЛЕНИЙ (скрипт deleted.sql)
# sql_script('/home/de1m/krlv/sql_scripts/deleted.sql', date_term)
# print('Loaded')

# 4. ОБНОВЛЕНИЕ МЕТАДАННЫХ (скрипт meta.sql)
print('Updating metadata...')
# sql_script('/home/de1m/krlv/sql_scripts/meta.sql', date_blk)
curs.execute(f"""insert INTO meta_all ( table_name, max_update_dt ) 
values ('fin', coalesce( timestamp '{max_date}', to_date( '1900.01.01', 'YYYY.MM.DD' ))) 
on conflict (table_name) do
    update set max_update_dt = timestamp'{max_date}' """)
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
# backup('C:/ETL/tvorog/' + path_fin, 'arhive', '.backup')
# # backup(path_trans, 'arhive', '.backup')
# # backup(path_blk, 'arhive', '.backup')
# print('Backup done')
print('=== ETL complete ====')