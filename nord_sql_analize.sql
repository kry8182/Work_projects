
-- Удаление строк начиная с некоего времени и установка в meta этого времени временем последнего изменения:
--  Использоавать очень осторожно!!!
DELETE FROM nord_1s WHERE time > to_timestamp('2023-03-17 05:41:56.062', 'YYYY-MM-DD HH24:MI:SS.MS' );
update  meta_all set max_update_dt = to_timestamp('2023-03-21 08:16:10.783', 'YYYY-MM-DD HH24:MI:SS.MS' ) where table_name = 'nord_1s'; 

-- Просмотр конкретного отсчета:
select * from nord_1s where time between to_timestamp('2023-03-21 00:00:01', 'YYYY-MM-DD HH24:MI:SS.MS' ) and to_timestamp('2023-03-21 00:00:02', 'YYYY-MM-DD HH24:MI:SS.MS' );


select 
time, 
front, 
lag(front) over (order by time) as lag_front,
lead(front) over (order by time) as  lead_front,
case when front = 1 and (lag(front) over (order by time) = 0) then time end as time_on,
case when front = 1 and (lead(front) over (order by time) = 0) then time end as time_off 
from nord_journal
where name = 'Alkali_not_ready'
order by time