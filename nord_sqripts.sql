
-- Удаление строк начиная с некоего времени и установка в meta этого времени временем последнего изменения:
DELETE FROM nord_1s WHERE time > to_timestamp('2023-03-17 05:41:56.062', 'YYYY-MM-DD HH24:MI:SS.MS' );
update  meta_all set max_update_dt = to_timestamp('2023-03-17 05:41:56.062', 'YYYY-MM-DD HH24:MI:SS.MS' ) where table_name = 'nord_1s'; 

--grafana nord_1s:
SELECT
  time AS "time",
  temperature_CIP  ,  
 temperature_na_vozvrate  ,  
-- Alarm_bit    ,  
 pressure_CIP ,  
CASE when potok < 50 then potok else null end potok  ,  
CASE when protok_CIP <= 120 then protok_CIP else null end vozvrat,
 temperature_CIP_2k   ,  
 temperature_na_vozvrate_2k   ,  
 pressure_CIP_2k  ,
-- Alarm_bit_2k ,  
CASE when protok_CIP_2k <= 120 then protok_CIP_2k else null end vozvrat_2k    ,  
CASE when potok_2k < 50 then potok_2k else null end potok_2k  
-- Show_0,
-- Show_0_2k
FROM nord_1s
WHERE
  $__timeFilter(time) 
ORDER BY 1