
-- Удаление строк начиная с некоего времени и установка в meta этого времени временем последнего изменения:
DELETE FROM nord_1s WHERE time > to_timestamp('2023-03-17 05:41:56.062', 'YYYY-MM-DD HH24:MI:SS.MS' );
update  meta_all set max_update_dt = to_timestamp('2023-03-21 08:16:10.783', 'YYYY-MM-DD HH24:MI:SS.MS' ) where table_name = 'nord_1s'; 

-- Просмотр конкретного отсчета:
select * from nord_1s where time between to_timestamp('2023-03-21 00:00:01', 'YYYY-MM-DD HH24:MI:SS.MS' ) and to_timestamp('2023-03-21 00:00:02', 'YYYY-MM-DD HH24:MI:SS.MS' );

--grafana nord_1s с others:
SELECT
 time AS "time",
 temperature_CIP  ,  
 temperature_na_vozvrate  ,  
 CASE when Alarm_bit then 50 else 0 end alarm_bit  , 
 pressure_CIP ,  
 -- CASE when potok < 50 then potok else null end potok  ,
 potok,  
 CASE when protok_CIP <= 120 then protok_CIP else null end vozvrat,
 temperature_CIP_2k   ,  
 temperature_na_vozvrate_2k   ,  
 pressure_CIP_2k  ,
 CASE when protok_CIP_2k <= 120 then protok_CIP_2k else null end vozvrat_2k    ,  
 -- CASE when potok_2k < 50 then potok_2k else null end potok_2k 
 potok_2k, 
 state,
 state_moika_2k,
 cast(null as real) as out_current_potok_2k_hz
FROM nord_1s
UNION 
select 
  time AS "time",
  temperature_CIP  ,  
  temperature_na_vozvrate  ,  
  CASE when Alarm_bit then 50 else 0 end alarm_bit  , 
  pressure_CIP ,  
--CASE when potok < 50 then potok else null end potok  ,
  potok,  
  CASE when protok_CIP <= 120 then protok_CIP else null end vozvrat,
  temperature_CIP_2k   ,  
  temperature_na_vozvrate_2k   ,  
  pressure_CIP_2k  ,
  CASE when protok_CIP_2k <= 120 then protok_CIP_2k else null end vozvrat_2k    ,  
--CASE when potok_2k < 50 then potok_2k else null end potok_2k 
  potok_2k, 
  null,
  null,
  null
FROM nord_1min
UNION
select 
 time,
 null,
 null,
 null,
 null,
 null,
 null,
 null,
 null,
 null,
 null,
 null,
 null,
 null,
 out_current_potok_2k / 8000 * 50 
from nord_other
WHERE
-- time > to_timestamp('2023-03-25 05:41:56.062', 'YYYY-MM-DD HH24:MI:SS.MS' );
  $__timeFilter(time) 
ORDER BY 1


--grafana nord_1s без others:
SELECT
 time AS "time",
 temperature_CIP  ,  
 temperature_na_vozvrate  ,  
 CASE when Alarm_bit then 50 else 0 end alarm_bit  , 
 pressure_CIP ,  
 -- CASE when potok < 50 then potok else null end potok  ,
 potok,  
 CASE when protok_CIP <= 120 then protok_CIP else null end vozvrat,
 temperature_CIP_2k   ,  
 temperature_na_vozvrate_2k   ,  
 pressure_CIP_2k  ,
 CASE when protok_CIP_2k <= 120 then protok_CIP_2k else null end vozvrat_2k    ,  
 -- CASE when potok_2k < 50 then potok_2k else null end potok_2k 
 potok_2k, 
 state,
 state_moika_2k
 FROM nord_1s
UNION ALL
select 
  time AS "time",
  temperature_CIP  ,  
  temperature_na_vozvrate  ,  
  CASE when Alarm_bit then 50 else 0 end alarm_bit  , 
  pressure_CIP ,  
--CASE when potok < 50 then potok else null end potok  ,
  potok,  
  CASE when protok_CIP <= 120 then protok_CIP else null end vozvrat,
  temperature_CIP_2k   ,  
  temperature_na_vozvrate_2k   ,  
  pressure_CIP_2k  ,
  CASE when protok_CIP_2k <= 120 then protok_CIP_2k else null end vozvrat_2k    ,  
--CASE when potok_2k < 50 then potok_2k else null end potok_2k 
  potok_2k, 
  null,
  null
FROM nord_1min

WHERE
  $__timeFilter(time) 
ORDER BY 1;
select 
 time,
 out_current_potok_2k / 8000 * 50 as out_current_potok_2k_hz
from nord_other
WHERE
  $__timeFilter(time) 
ORDER BY 1;


--Графана nord_1s+1min JOIN  out_current_potok_2k:

SELEct 
 time AS "time",
 temperature_CIP  ,  
 temperature_na_vozvrate  ,  
 alarm_bit  , 
 pressure_CIP ,  
potok,  
 vozvrat,
 temperature_CIP_2k   ,  
 temperature_na_vozvrate_2k   ,  
 pressure_CIP_2k  ,
 vozvrat_2k    ,  
potok_2k, 
state,
state_moika_2k,
out_current_potok_2k / 8000 * 50 as out_current_potok_2k_hz
 from
(SELECT
  time AS "time",
    temperature_CIP  ,  
 temperature_na_vozvrate  ,  
-- Alarm_bit    ,
CASE when Alarm_bit then 50 else 0 end alarm_bit  , 
 pressure_CIP ,  
-- CASE when potok < 50 then potok else null end potok  ,
potok,  
CASE when protok_CIP <= 120 then protok_CIP else null end vozvrat,
 temperature_CIP_2k   ,  
 temperature_na_vozvrate_2k   ,  
 pressure_CIP_2k  ,
-- Alarm_bit_2k ,  
CASE when protok_CIP_2k <= 120 then protok_CIP_2k else null end vozvrat_2k    ,  
-- CASE when potok_2k < 50 then potok_2k else null end potok_2k 
potok_2k, 
state,
state_moika_2k
FROM nord_1s
UNION 
select 
  time AS "time",
  temperature_CIP  ,  
 temperature_na_vozvrate  ,  
-- Alarm_bit    ,
CASE when Alarm_bit then 50 else 0 end alarm_bit  , 
 pressure_CIP ,  
-- CASE when potok < 50 then potok else null end potok  ,
potok,  
CASE when protok_CIP <= 120 then protok_CIP else null end vozvrat,
 temperature_CIP_2k   ,  
 temperature_na_vozvrate_2k   ,  
 pressure_CIP_2k  ,
-- Alarm_bit_2k ,  
CASE when protok_CIP_2k <= 120 then protok_CIP_2k else null end vozvrat_2k    ,  
-- CASE when potok_2k < 50 then potok_2k else null end potok_2k 
potok_2k, 
null,
null
FROM nord_1min )t
LEFT join (select time as tm, out_current_potok_2k from nord_other) o
on o.tm between t.time - interval '500 milliseconds' and t.time + interval '500 milliseconds'
WHERE
1=1 
and  $__timeFilter(time) 
-- and t.time > to_timestamp('2023-03-25 15:20:13.364', 'YYYY-MM-DD HH24:MI:SS.MS' )
-- And out_current_potok_2k is not null;
ORDER BY 1


-- grafana alkali:
SELECT
  time AS "time",
  concentration_alk,
  CASE when V114_open then 1 else 0 end V114_open  ,
  CASE when V212_open then 1 else 0 end V212_open ,
  CASE when V112_open then 1 else 0 end V112_open ,
  CASE when V107 then 1 else 0 end V107 ,
  CASE when h_urov_sheloch then 1 else 0 end h_urov_sheloch ,
  CASE when alkali_not_ready then 1 else 0 end alkali_not_ready,
  CASE when m_urov_sheloch then 1 else 0 end m_urov_sheloch,
  n_moek_alkali / 10 as n_moek
FROM nord_alkali
UNION 
select 
  time AS "time",
  concentration_alk,
  null,
  null,
  null,
  null,
  null,
  null,
  null,
  null
FROM nord_1min
WHERE
  $__timeFilter(time)
ORDER BY 1

-- grafana concentration_vozvr + клапана возврата + в.у. щелочи
 
select 
  time AS "time",
  CAST (null as real) as concentration_vozvr,
  CAST (null as real) as concentration_vozvr_2k,
  CASE when V212_open then 1 else 0 end V212_open ,
  CASE when V112_open then 1 else 0 end V112_open,
  CASE when h_urov_sheloch then 1 else 0 end h_urov_sheloch 
FROM nord_alkali
UNION
SELECT
  time AS "time",
  concentration_vozvr,
  concentration_vozvr_2k,
  null,
  null,
  null
FROM nord_alkali
WHERE
  $__timeFilter(time)
UNION 
select 
  time AS "time",
  concentration_vozvr,
  concentration_vozvr_2k,
  null,
  null,
  null
FROM nord_1min
WHERE
  $__timeFilter(time)
ORDER BY 1

--- определение момента неготовности щелочи (без использования журнала, через оконные функции)
with alkali_window as (
SELECT 
    time,
    alkali_not_ready,
    concentration_alk,
    n_moek_alkali,
    V107,
    lag(alkali_not_ready) over (order by time) as previous_not_ready,
    lag(V107) over (order by time) as previous_podpitka,
    lead(alkali_not_ready) over (order by time) as next_not_ready,
    lag(n_moek_alkali) over (order by time) as previous_n_moek_alkali,
    case when (lag(m_urov_sheloch) over (order by time))then 1 else 0 end as previous_m_urov_sheloch
    
FROM nord_alkali
WHERE 1=1
    and $__timeFilter(time)
-- and time > to_timestamp('2023-03-26 15:41:56.062', 'YYYY-MM-DD HH24:MI:SS.MS' )
order by 1
                   ) 
                        
--Таблица по щелочным мойкам

SELECT -- момент начала неготовности щелочи
   time
   ,concentration_alk
   ,previous_n_moek_alkali
   ,previous_m_urov_sheloch
FROM 
alkali_window
WHERE 1=1
and alkali_not_ready 
and not previous_not_ready
union all
SELECT  --момент конца подпитки (минимум концентрации)
   time 
   ,concentration_alk
   ,null
   ,null
FROM 
alkali_window
WHERE 1=1
and not V107 
and previous_podpitka
union all
SELECT -- момент конца приготовления
   time 
   ,concentration_alk
   ,n_moek_alkali
   ,null
FROM 
alkali_window
WHERE 1=1
and alkali_not_ready 
and not next_not_ready;


-- Изменение концентрации при последнем приготовлении
with alkali_window as (
SELECT 
    time,
    alkali_not_ready,
    concentration_alk,
    n_moek_alkali,
    V107,
    lag(alkali_not_ready) over (order by time) as previous_not_ready,
    lag(V107) over (order by time) as previous_podpitka,
    lead(alkali_not_ready) over (order by time) as next_not_ready,
    lag(n_moek_alkali) over (order by time) as previous_n_moek_alkali,
    case when (lag(m_urov_sheloch) over (order by time))then 1 else 0 end as previous_m_urov_sheloch
    
FROM nord_alkali
WHERE 1=1
--     and $__timeFilter(time)
    and time > to_timestamp('2023-03-26 15:41:56.062', 'YYYY-MM-DD HH24:MI:SS.MS' )
order by 1
                   ) 
                        
--минимум концентрации во время пригтовления
select max, min, max - min as padenie_conc  from
(select  min(concentration_alk) from nord_alkali
WHERE
time between
(SELECT -- момент начала неготовности щелочи
   time
FROM 
alkali_window
WHERE 1=1
and alkali_not_ready 
and not previous_not_ready) 

and 

(SELECT -- момент конца приготовления
   time 
FROM 
alkali_window
WHERE 1=1
and alkali_not_ready 
and not next_not_ready) 
) t1

join

(SELECT -- момент начала неготовности щелочи
   max(concentration_alk)
FROM 
alkali_window
WHERE 1=1
and alkali_not_ready 
and not previous_not_ready) t2
on 1=1
