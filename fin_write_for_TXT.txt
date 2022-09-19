#!/usr/bin/python

import sys
import pandas
import psycopg2
import os
import datetime
import re



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



# Получение путей к файлам
file_name = input('Введите месяц в формате mmyyyy:') 
print('Reading files...')
path = ''
lstdir = os.listdir(r"C:/ETL/fin")
for i in lstdir:
    if i.find(file_name + '.txt') > -1: 
        path = i
if path > '': print(path)
else: 
     print ('file is not found')
     sys.exit()



# Чтение файла
f = open('C:/ETL/fin/' + path,'r', encoding='UTF-8')
s = f.read()
f.close()
print('Readed')
date = '01/'+ path[0:2] + '/' + path[2:-4] #Дату берем из названия
print(date)

# Коннектимся с сервером
print('=== ETL start ===')
print('Connecting...')
conn = create_connection("kiplocal", "postgres", "111111", "localhost", "5432")
conn.autocommit = False
curs = conn.cursor()
print('Connected')

# Проверка на отсутсвие данных за этот месяц и удаление при наличии и согласии пользовотеля
curs.execute(f'''select count(m_date) from fin
where m_date = timestamp '{date}' ''')
month = curs.fetchall()
print(f'в базе на этот месяц найдено {month[0][0]} cтрок')
again = True
while again and month[0][0] > 0:
        yes_or_no = input('За этот месяц есть данные. Заменить их (y/n)?:')
        if yes_or_no == 'n': 
            curs.close()
            conn.close()
            print('Connect closed, exit')
            sys.exit()
        elif yes_or_no =='y': again = False

if month[0][0] > 0: curs.execute(f'''delete from fin where m_date = timestamp '{date}' ''')


# Выделение данных 
s = s.replace('\n','')
s = s.replace(' ','')
s = s.lower()
lst = s.split(',')
d = {   'лизе':0,'x':0,'топл':0,'мег':0,'мег2':0,'бил':0,'интернет':0,
        '*':0,'прод':0,'ег':0,'запас':0,'апт':0,'подарки':0,'угощ':0,'хозтов':0,
        'необх':0,'расх':0,'образован':0,'коты':0,'детск.кап':0,'детск.тек':0,'детск.апт':0,
        'детск.мед':0,'одежда':0,'канцтов':0,
        'принтер':0,'профилакт':0,'мед':0,'модерн':0,'проч':0,
        'Т3':0,'датсун':0,'штраф':0,'б':0,'ремонт':0,
        'убыт':0,'сх':0,'пстгу':0,'транспорт':0,'спорт':0,
        'хобби':0,'досуг':0,'кварт':0,'налог':0,'книги':0,
        'ревивал':0,'стройка':0,'инструмент':0,      'доставка':0,
        'вдолг':0,'отпуск':0,'ларгус':0,'наем':0,'инф.дер':0,
        'обсл.дом':0,'авто':0,'колхоз':0,'автодор':0,'инф.всев':0,
        'пособ':0,'зплиза':0,'зп':0,'ах':0,'ак':0,
        'рента':0,'подаркинам':0,'бонус':0,'%':0,'возврат':0
     }
print(lst)
for i in lst:
    name = re.findall(r'[а-я,А-Я,ёЁ.%*]+',i)
    num = re.findall(r'[0-9,+,-]+',i)
    if num[0].find('+') == -1: 
        val = int(num[0])
    else: 
        summands = num[0].split('+')
        for j in range(len(summands)): summands[j] = int(summands[j])
        val = sum(summands)
    if name[0] in d: 
        d[name[0]] = d[name[0]] + val
    else: 
        print('неизвестное наименование: ', name[0])
        sys.exit()
        
# Теперь в словаре d содержатся пары категория:сумма      
print(d)

# Вносим данные в БД
print('Loading changes to DWH...')
c = 0
curs.execute(f''' insert into fin 
    (m_date,
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
    vozvrat
    ) 
values (timestamp'{date}',
        '{d['лизе']}','{d['x']}','{d['топл']}','{d['мег']}','{d['мег2']}',
        '{d['бил']}','{d['интернет']}','{d['*']}','{d['прод']}','{d['ег']}',
        '{d['запас']}','{d['апт']}','{d['подарки']}','{d['угощ']}','{d['хозтов']}',
        '{d['необх']}','{d['расх']}','{d['образован']}','{d['коты']}','{d['детск.кап']}',
        '{d['детск.тек']}','{d['детск.апт']}','{d['детск.мед']}','{d['одежда']}','{d['канцтов']}',
        '{d['принтер']}','{d['профилакт']}','{d['мед']}','{d['модерн']}','{d['проч']}',
        '{d['Т3']}','{d['датсун']}','{d['штраф']}','{d['б']}','{d['ремонт']}',
        '{d['убыт']}','{d['сх']}','{d['пстгу']}','{d['транспорт']}','{d['спорт']}',
        '{d['хобби']}','{d['досуг']}','{d['кварт']}','{d['налог']}','{d['книги']}',
        '{d['ревивал']}','{d['стройка']}','{d['инструмент']}',      '{d['доставка']}',
        '{d['вдолг']}','{d['отпуск']}','{d['ларгус']}','{d['наем']}','{d['инф.дер']}',
        '{d['обсл.дом']}','{d['авто']}','{d['колхоз']}','{d['автодор']}','{d['инф.всев']}',
        '{d['пособ']}','{d['зплиза']}','{d['зп']}','{d['ах']}','{d['ак']}',
        '{d['рента']}','{d['подаркинам']}','{d['бонус']}','{d['%']}','{d['возврат']}'
        )
 ''')
c =+ 1
print(c,' rows inserted')
print('Updating metadata...')
# sql_script('/home/de1m/krlv/sql_scripts/meta.sql', date_blk)
curs.execute(f"""insert INTO meta_all ( table_name, max_update_dt ) 
values ('fin', coalesce( timestamp '{date}', to_date( '1900.01.01', 'YYYY.MM.DD' ))) 
on conflict (table_name) do
    update set max_update_dt = timestamp'{date}' """)
print('Updated')

# Подбитие итогов
curs.execute(f''' update fin set itogo = (select (lise+
    x+
    topl+
    meg+
    meg2+
    bil+
    internet+
    gkh+
    prod+
    eg+
    zapas+
    apteka+
    podarki+
    ugoshen+
    hoztovar+
    neobhodimoe+
    rashodniki+
    obrazovan+
    kats+
    detsk_kapital+
    detsk_tekush+
    detsk_apt+
    detsk_med+
    odegda_obuv+
    kanctov+
    rash_printera+
    profilakt+
    medicine+
    modernization+
    proch+
    T3+
    matiz+
    shtrafs+
    pogertv+
    remonts+
    ubytki+
    selhoz+
    PSTGU+
    transport+
    sport+
    hobbi+
    dosug+
    kvart+
    nalogi+
    knigi+
    revival+
    stroyka+
    instrument+
    dostavka+
    vdolg+
    otpusk+
    largus+
    zp+
    derev_infrastructur+
    obsl_doma+
    obsheavto+
    kolhoz+
    avtodor+
    vsevologsk_infrastructur) from fin
where m_date = timestamp '{date}')
where m_date = timestamp '{date}' 
''')

curs.execute(f'''
update fin set itogo_in = (select (posobia+
    liza_in+
    lesha_in+
    x_cpsh+
    auto_comp+
    rent+
    podarki_in+
    bonus+
    procent+
    vozvrat) from fin
where m_date = timestamp '{date}')
where m_date = timestamp '{date}'
''')



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