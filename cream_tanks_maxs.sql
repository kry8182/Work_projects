
select * from table1
where table1.current_time > to_timestamp( '2022/03/09', 'YYYY/MM/DD HH24:MI' );

select 
max(t_tank03) t3,
max(t_tank01) t1,
max(t_tank07) t7,
max(t_tank06) t6,
max(t_tank26) t26,
max(t_tank13) t13,
max(t_tank04) t4,
max(t_tank14) t14,
max(t_tank12) t12,
max(t_tank18) t18,
max(t_tank17) t17,
max(t_tank19) t19,
max(t_tank21) t21,
max(t_tank15) t15,
max(t_tank16) t16,
max(t_tank20) t20
from table1
where table1.current_time between to_timestamp('2022/03/09 00:00','YYYY/MM/DD HH24:MI') and to_timestamp('2022/03/10 00:00','YYYY/MM/DD HH24:MI')
union all
select 
max(t_tank03) t3,
max(t_tank01) t1,
max(t_tank07) t7,
max(t_tank06) t6,
max(t_tank26) t26,
max(t_tank13) t13,
max(t_tank04) t4,
max(t_tank14) t14,
max(t_tank12) t12,
max(t_tank18) t18,
max(t_tank17) t17,
max(t_tank19) t19,
max(t_tank21) t21,
max(t_tank15) t15,
max(t_tank16) t16,
max(t_tank20) t20
from table1
where table1.current_time between to_timestamp('2022/03/08 00:00','YYYY/MM/DD HH24:MI') and to_timestamp('2022/03/09 00:00','YYYY/MM/DD HH24:MI')
union all
select 
max(t_tank03) t3,
max(t_tank01) t1,
max(t_tank07) t7,
max(t_tank06) t6,
max(t_tank26) t26,
max(t_tank13) t13,
max(t_tank04) t4,
max(t_tank14) t14,
max(t_tank12) t12,
max(t_tank18) t18,
max(t_tank17) t17,
max(t_tank19) t19,
max(t_tank21) t21,
max(t_tank15) t15,
max(t_tank16) t16,
max(t_tank20) t20
from table1
where table1.current_time between to_timestamp('2022/03/07 00:00','YYYY/MM/DD HH24:MI') and to_timestamp('2022/03/08 00:00','YYYY/MM/DD HH24:MI')


-- Средняя температура стерилизации
select 
cast(avg(t_tank04) as numeric(5,2)) t4
from (select t_tank04 from table1 where t_tank04 > 80 
and table1.current_time between to_timestamp('2022/03/09 00:00','YYYY/MM/DD HH24:MI') and to_timestamp('2022/03/10 00:00','YYYY/MM/DD HH24:MI')
     ) t







