#!/usr/bin/python
# расчет значений и загрузка в таблицу steril_integals_on_date данных за интервал времени

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

# Коннектимся с сервером
print('=== start ===')
print('Connecting...')
conn = create_connection("kip", "kip", "kip", "192.168.100.223", "5432")
conn.autocommit = False
curs = conn.cursor()
print('Connected')

# #Очистка STG (скрипт truncate.sql)
# print('Truncating STG-tables...')
# # sql_script('/home/de1m/krlv/sql_scripts/truncate.sql','')
# curs.execute("truncate table tvorog_alkali_wash_stg")
# print('Truncated')


#Считываем дату последней загрузки из таблицы метаданных
curs.execute("select max_update_dt from meta_all where table_name = 'steril_integals_on_date'")
date_from_meta = curs.fetchall()
if len(date_from_meta) > 0:
    t = date_from_meta[0][0]
    date_from_meta_str = t.strftime("%Y/%m/%d %H:%M:%S")
else: 
    t = datetime.datetime(1900,1,1,0,0)
    date_from_meta_str = t.strftime("%Y/%m/%d %H:%M:%S")
print('max date in meta:', date_from_meta_str)

# Ввод переменных
date = input('Введите дату расчета: ')
n = int(input('+ сколько дней еще расчитать: '))
temp_threshold = int(input('порог температуры: '))
tt = (input('номер танка: '))
if tt.startswith('0'): column_name = 't' + tt[-1]
else: column_name = 't' + tt

# Расчитываем и вставляем данные в таблицу
for i in range(0,n+1):
    interval = str(i)
    curs.execute(f''' insert into steril_integals_on_date( date, {column_name} ) 
    select 
        timestamp '{date}' + interval '{interval}' day,
        avg(t)*count(t)*10 as {column_name}_integral
    from (select 
        t_tank{tt} as t
       from table1 where t_tank{tt} > {temp_threshold}
       and table1.current_time between (timestamp '{date}' + interval '{interval}' day - interval '1' day)  and (timestamp '{date}' + interval '{interval}' day)
         ) tab
        on conflict (date) do
        update set {column_name} = (select avg(t)*count(t)*10 as {column_name}_integral
            from (select 
            t_tank{tt} as t
            from table1 where t_tank{tt} > {temp_threshold}
            and table1.current_time between (timestamp '{date}' + interval '{interval}' day - interval '1' day)  and (timestamp '{date}' + interval '{interval}' day)
            ) tab       )
                 ''' )
print(i + 1,' rows inserted')

# Выводим результат
curs.execute(f'''select cast(max({column_name}) as numeric(7,0)), cast(avg({column_name})as numeric(7,0)) from steril_integals_on_date
where  date between (timestamp '{date}' - interval '1' day)  and (timestamp '{date}' + interval '{interval}' day) ''')
answer = curs.fetchall()
print('Максимум =', answer[0][0])
print('Cреднее =', answer[0][1])
print('Макс/Cредн =', answer[0][0] / answer[0][1])



#  ОБНОВЛЕНИЕ МЕТАДАННЫХ 
print('Updating metadata...')
# sql_script('/home/de1m/krlv/sql_scripts/meta.sql', date_blk)
curs.execute("select max(date) from steril_integals_on_date")
max_date_from_DWH = curs.fetchall();
t = max_date_from_DWH[0][0]
max_date_from_DWH_str = t.strftime("%Y/%m/%d %H:%M:%S")
print(f'New max_date_to_meta:{max_date_from_DWH_str}')
curs.execute(f"""insert INTO meta_all ( table_name, max_update_dt ) 
values ('steril_integals_on_date', coalesce( to_timestamp( '{max_date_from_DWH_str}', 'YYYY/MM/DD HH24:MI:SS' ), to_date( '1900.01.01', 'YYYY.MM.DD' ))) 
on conflict (table_name) do
    update set max_update_dt = to_timestamp('{max_date_from_DWH_str}', 'YYYY/MM/DD HH24:MI:SS' )""")
print('Updated')

# Фиксируем изменения
print('Commit')
conn.commit()

# Закрываем коннект
curs.close()
conn.close()
print('Connect closed')
print('===== End =====')

