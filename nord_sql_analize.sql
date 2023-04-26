
-- Удаление строк начиная с некоего времени и установка в meta этого времени временем последнего изменения:
--  Использоавать очень осторожно!!!
DELETE FROM nord_journal WHERE time > to_timestamp('2023-03-17 05:41:56.062', 'YYYY-MM-DD HH24:MI:SS.MS' );
update  meta_all set max_update_dt = to_timestamp('2023-04-14 05:00:45.001', 'YYYY-MM-DD HH24:MI:SS.MS' ) where table_name = 'nord_journal'; 

-- Просмотр конкретного отсчета:
select * from nord_1s where time between to_timestamp('2023-03-21 00:00:01', 'YYYY-MM-DD HH24:MI:SS.MS' ) and to_timestamp('2023-03-21 00:00:02', 'YYYY-MM-DD HH24:MI:SS.MS' );

--Поиск дубликатов!!
select * FROM nord_journal WHERE ctid NOT IN
(SELECT max(ctid) FROM nord_journal GROUP BY nord_journal.*);

-- в пределах секунды от отчсета
on n.time between a.eff_from  and  a.eff_from + interval '990 milliseconds';

-- Разбивка по диапазонам приготовления щ.

-- WITH  alkali_not_ready_period as (
-- SELECT * FROM (
-- select 
-- name,
-- front,
-- eff_from,
-- lead(eff_to) over (order by time) as eff_to
-- from (
-- select
-- name,
-- time, 
-- front, 
-- case when (front = 1 and (lag(front) over (order by time) = 0))
--     or (front = 0 and (lag(front) over (order by time) = 1)) then time end as eff_from,
-- case when (front = 1 and (lead(front) over (order by time) = 0))
--     or (front = 0 and (lead(front) over (order by time) = 1)) then time end as eff_to
-- from nord_journal
-- where name = 'Alkali_not_ready' 
-- and $__timeFilter(time)
-- order by time
--   ) t
--         )tt
-- WHERE eff_from is not null 
--         and front = 1 )

 
--таблица характеристик приготовлений щ.: разницы концентраций, длительность дозирований и подпиток 
--  адекватные данные есть с 10.04.2023

SELECT 
  to_char( cook_and_doz.eff_from, 'YYYY-MM-DD HH24:MI:SS') as start
, to_char(cook_and_doz.eff_to, 'YYYY-MM-DD HH24:MI:SS') as end 
, max(duration_cooking) as duration_cooking
, max(count_doz) as n_doz
, max(duration_doz) as duration_doz
, sum(v7.eff_to - v7.eff_from) as duration_filling
, max(conc_start) as conc_start
, max(conc_end) as conc_end
, max(delta_conc) as delta_conc
, max(min_conc) as min_conc
, max(reduc_сonc_when_filling) as reduc_сonc_when_filling

FROM (
select 
cook.eff_from as eff_from
, cook.eff_to as eff_to
, max(duration_cooking) as duration_cooking
, case when max(v.eff_to) is null then 0 else count(*) end as count_doz
, sum(v.eff_to - v.eff_from) as duration_doz
, max(conc_start) as conc_start
, max(conc_end) as conc_end
, max(delta_conc) as delta_conc
, max(min_conc) as min_conc
, max(reduc_сonc_when_filling) as reduc_сonc_when_filling

from (
select 
  eff_from
, eff_to
, eff_to - eff_from as duration_cooking
, max(conc_alk_start) as conc_start
, max(conc_alk_end) as conc_end
, max(conc_alk_end - conc_alk_start) as delta_conc
, min(concentration_alk) as min_conc
, max(conc_alk_start) - min(concentration_alk) as reduc_сonc_when_filling
FROM (
select eff_from, eff_to 
,concentration_alk
,first_value(concentration_alk) over w as conc_alk_start
,last_value(concentration_alk) over w as conc_alk_end
from
alkali_not_ready_period a
left join
nord_alkali n
on time between eff_from and eff_to
where $__timeFilter(eff_from)
  window w as (
  partition by eff_from
  order by time
  rows between unbounded preceding and unbounded following)
      ) ttt
GROUP BY eff_from, eff_to

            ) cook
left join
V114_open_period v
on v.eff_from > cook.eff_from and v.eff_to < cook.eff_to
GROUP BY cook.eff_from, cook.eff_to
            ) cook_and_doz
left join
V107_open_period v7
on v7.eff_from > cook_and_doz.eff_from and v7.eff_to < cook_and_doz.eff_to
GROUP BY cook_and_doz.eff_from, cook_and_doz.eff_to
ORDER BY cook_and_doz.eff_from desc;


-- то же самое, но без подпиток (временно сохранен старый вариант):
SELECT 
 to_char( cook.eff_from, 'YYYY-MM-DD HH24:MI:SS') as start
, to_char(cook.eff_to, 'YYYY-MM-DD HH24:MI:SS') as end
, max(duration_cooking) as duration_cooking
, max(conc_start) as conc_start
, max(conc_end) as conc_end
, max(delta_conc) as delta_conc
, max(min_conc) as min_conc
, max(reduc_сonc_when_filling) as reduc_сonc_when_filling
, case when max(v.eff_to) is null then 0 else count(*) end as count_doz
, sum(v.eff_to - v.eff_from) as duration_doz
FROM (
SELECT 
  eff_from
, eff_to
, eff_to - eff_from as duration_cooking
, max(conc_alk_start) as conc_start
, max(conc_alk_end) as conc_end
, max(conc_alk_end - conc_alk_start) as delta_conc
, min(concentration_alk) as min_conc
, max(conc_alk_start) - min(concentration_alk) as reduc_сonc_when_filling
FROM (
select eff_from, eff_to 
,concentration_alk
,first_value(concentration_alk) over w as conc_alk_start
,last_value(concentration_alk) over w as conc_alk_end
from
alkali_not_ready_period a
left join
nord_alkali n
on time between eff_from and eff_to
  window w as (
  partition by eff_from
  order by time
  rows between unbounded preceding and unbounded following)
      ) ttt
GROUP BY eff_from, eff_to

            ) cook
left join
V114_open_period v
on v.eff_from > cook.eff_from and v.eff_to < cook.eff_to
GROUP BY cook.eff_from, cook.eff_to
ORDER BY cook.eff_from desc

-- ///////////////////////////////////////////////////////////////////////

CREATE  or replace VIEW  V107_open_period AS (
select * from (
SELECT 
name,
front,
eff_from,
lead(eff_to) over (order by time) as eff_to
FROM (
select
name,
time, 
front, 
case when (front = 1 and (lag(front) over (order by time) = 0))
    or (front = 0 and (lag(front) over (order by time) = 1)) then time end as eff_from,
case when (front = 1 and (lead(front) over (order by time) = 0))
    or (front = 0 and (lead(front) over (order by time) = 1)) then time end as eff_to
from nord_journal
where name = 'V107_open' 
 -- and $__timeFilter(time)
order by time
  ) t
        )tt
WHERE eff_from is not null 
        and front = 1 );

CREATE  VIEW  Alkali_not_ready_period AS 
(
SELECT * FROM (
select 
name,
front,
eff_from,
lead(eff_to) over (order by time) as eff_to
from (
select
name,
time, 
front, 
case when (front = 1 and (lag(front) over (order by time) = 0))
    or (front = 0 and (lag(front) over (order by time) = 1)) then time end as eff_from,
case when (front = 1 and (lead(front) over (order by time) = 0))
    or (front = 0 and (lead(front) over (order by time) = 1)) then time end as eff_to
from nord_journal
where name = 'Alkali_not_ready' 
-- and $__timeFilter(time)
order by time
  ) t
        )tt
WHERE eff_from is not null 
        and front = 1 );

CREATE or replace VIEW  Alkali_or_acid_program_2k AS 
(
SELECT * FROM (
select 
name,
front,
eff_from,
lead(eff_to) over (order by time) as eff_to
from (
select
name,
time, 
front, 
case when (front = 1 and (lag(front) over (order by time) = 0))
    or (front = 0 and (lag(front) over (order by time) = 1)) then time end as eff_from,
case when (front = 1 and (lead(front) over (order by time) = 0))
    or (front = 0 and (lead(front) over (order by time) = 1)) then time end as eff_to
from nord_journal
where name in ('Alkali_start_2k','Acid_start_2k')
-- and $__timeFilter(time)
order by time
  ) t
        )tt
WHERE eff_from is not null 
        and front = 1 );

CREATE or replace VIEW  Step_2_2k_to_step_3_2k AS 
(
 SELECT eff_from, eff_to, l.name as wash from 
 (
  SELECT eff_from, eff_to from (  
    SELECT 
      name
      ,eff_from
      ,case 
        when lead(name) over (order by eff_from) = 'Step_3_2k' then lead(eff_to) over (order by eff_from)
       end as eff_to
    FROM (   
      SELECT * FROM (
        select 
        name,
        front,
        eff_from,
        lead(eff_to) over (order by time) as eff_to
        from (
        select
        name,
        time, 
        front, 
        case when (front = 1 and (lag(front) over (order by time) = 0))
          or (front = 0 and (lag(front) over (order by time) = 1)) then time end as eff_from,
        case when (front = 1 and (lead(front) over (order by time) = 0))
          or (front = 0 and (lead(front) over (order by time) = 1)) then time end as eff_to
        from nord_journal
        where name in ('Step_2_2k')
        -- and $__timeFilter(time)
        order by time
          ) t
        )tt
      WHERE eff_from is not null 
          and front = 1 
      UNION ALL
        SELECT * FROM (
        select 
        name,
        front,
        eff_from,
        lead(eff_to) over (order by time) as eff_to
        from (
        select
        name,
        time, 
        front, 
        case when (front = 1 and (lag(front) over (order by time) = 0))
          or (front = 0 and (lag(front) over (order by time) = 1)) then time end as eff_from,
        case when (front = 1 and (lead(front) over (order by time) = 0))
          or (front = 0 and (lead(front) over (order by time) = 1)) then time end as eff_to
        from nord_journal
        where name in ('Step_3_2k')
        -- and $__timeFilter(time)
        order by time
          ) t
          )tt
      WHERE eff_from is not null 
          and front = 1
      )ttt
  )tttt
  WHERE name = 'Step_2_2k'
) ss
JOIN
Alkali_or_acid_list_2k l
ON ss.eff_from > l.eff_from and ss.eff_to < l.eff_to
order by eff_from
);



CREATE or replace VIEW  Alkali_or_acid_list_2k AS 
SELECT 
j.name 
,eff_from
,eff_to
,p.name as type_prog
FROM
Alkali_or_acid_program_2k p
left join
nord_journal j
on 1=1
and j.time between p.eff_from - interval '1 second' and p.eff_from + interval '1 second'
and j.name in (
 'Moika_T4_2k'
,'Moika_T5_2k'
,'Moika_T6_2k'
,'Moika_T7_2k'
,'Moika_T8_2k'
,'Moika_T9_2k'
,'Moika_T10_2k'
,'Moika_T11_2k'
,'Moika_T12_2k'
,'Moika_T13_2k'
,'Moika_T14_2k'
,'Moika_T15_2k'
,'Moika_T16_2k'
,'Moika_T17_2k'
,'Moika_T18_2k'
,'Moika_line_2k'
,'Step_steril_2k'
,'Moika_line2_2k'
,'Moika_line_2k'
,'Moika_line_razv_2k'
              )
and j.front = 1
order by eff_from

-- Таблица моек
SELECT * from (
SELECT 
min(n_moek) as N -- чтобы различать номера моек при одновременном запуске контуров
,max(name) as name
, case when max(type_prog) = 'Acid_start_2k' then 'acid' else case when max(type_prog) = 'Alkali_start_2k' then 'alk' end end as type
, to_char( eff_from, 'YYYY-MM-DD HH24:MI:SS') as start
, to_char(eff_to, 'YYYY-MM-DD HH24:MI:SS') as end
, eff_to - eff_from as duration_wash
, cast (max(conc_alk_start - conc_alk_end) as numeric(4,3)) as delta_conc
, max(conc_alk_start) as conc_start
, max(conc_alk_end) as conc_end

FROM (
select eff_from, eff_to
,a.name as name
,a.type_prog as type_prog
,concentration_alk
,n_moek_alkali as n_moek 
,first_value(concentration_alk) over w as conc_alk_start
,last_value(concentration_alk) over w as conc_alk_end
from
Alkali_or_acid_list_2k  a
left join
nord_alkali n
on time between eff_from and eff_to
  window w as (
  partition by eff_from
  order by time
  rows between unbounded preceding and unbounded following)
) ttt
where $__timeFilter(eff_from)
GROUP BY eff_from, eff_to
UNION All 
SELECT 
max(n_moek) as N -- чтобы различать номера моек при одновременном запуске контуров
,max(name) as name
, case when max(type_prog) = 'Acid_start' then 'acid' else case when max(type_prog) = 'Alkali_start' then 'alk' end end as type
, to_char( eff_from, 'YYYY-MM-DD HH24:MI:SS') as start
, to_char(eff_to, 'YYYY-MM-DD HH24:MI:SS') as end
, eff_to - eff_from as duration_wash
, cast (max(conc_alk_start - conc_alk_end) as numeric(4,3)) as delta_conc
, max(conc_alk_start) as conc_start
, max(conc_alk_end) as conc_end

FROM (
select eff_from, eff_to
,a.name as name
,a.type_prog as type_prog
,concentration_alk
,n_moek_alkali as n_moek 
,first_value(concentration_alk) over w as conc_alk_start
,last_value(concentration_alk) over w as conc_alk_end
from
Alkali_or_acid_list  a
left join
nord_alkali n
on time between eff_from and eff_to
  window w as (
  partition by eff_from
  order by time
  rows between unbounded preceding and unbounded following)
) ttt
where $__timeFilter(eff_from)
GROUP BY eff_from, eff_to) tab_union
ORDER by start desc


-- Таблица длительностей возврата 2k 
select 
v.eff_from
, v.eff_to
, v.eff_to-v.eff_from as duration_open
, avg(v.eff_to-v.eff_from) over w
,lst.name
from V212_open_period v
-- where $__timeFilter(eff_from) 
join 
(select * from Alkali_or_acid_list
union All
select * from Alkali_or_acid_list_2k) lst
on v.eff_from > lst.eff_from and v.eff_to < lst.eff_to
window w as (
  -- partition by eff_from
  order by v.eff_from
  rows between unbounded preceding and unbounded following) 
order by v.eff_from desc

-- Таблица длительностей подачи щелочи 2k 

select 
eff_from
, eff_to
, eff_to-eff_from as duration_open
, avg(eff_to-eff_from) over w
from V103_open_period v
where $__timeFilter(eff_from) 
join 
Step_2_2k_or_step_3_2k s
on v.eff_from < s.eff_from and s.eff_to < v.
window w as (
  -- partition by eff_from
  order by eff_from
  rows between unbounded preceding and unbounded following) 
order by eff_from desc


-- Таблица объемов возвращенной щелочи
select 
max(lst.name)
,v.eff_from, v.eff_to
,sum(s.Protok_CIP_2k - 8) as volume 
,v.eff_to-v.eff_from as duration
from
(
  select * from (
  SELECT 
  name,
  front,
  eff_from,
  lead(eff_to) over (order by time) as eff_to
  FROM (
    select
    name,
    time, 
    front, 
    case when (front = 1 and (lag(front) over (order by time) = 0))
      or (front = 0 and (lag(front) over (order by time) = 1)) then time end as eff_from,
    case when (front = 1 and (lead(front) over (order by time) = 0))
      or (front = 0 and (lead(front) over (order by time) = 1)) then time end as eff_to
    from 
    nord_journal 
    where name = 'V212_open' 
     and $__timeFilter(time)
    order by time
      ) t
      )tt
  WHERE eff_from is not null 
      and front = 1 
) v
JOIN
nord_1s s
ON s.time between  v.eff_from and v.eff_to - interval '11 second' --последние 11 с сливаются на пол
    and s.Protok_CIP_2k > 8
join 
Alkali_or_acid_list_2k lst
on v.eff_from > lst.eff_from and v.eff_to < lst.eff_to
GROUP BY v.eff_from, v.eff_to


-- объемы и средние относительные концентрации (возврат/бак) на возврате:
CREATE  or replace VIEW alkali_back_in_bak_period AS (
select eff_from, eff_to
,sum(s.Protok_CIP_2k - 8) as volume 
,avg(Concentration_vozvr_2k/concentration_alk) avg_otnoshen_conc
from
(
(
  select * from (
  SELECT 
  name,
  front,
  eff_from,
  lead(eff_to) over (order by time) as eff_to
  FROM (
    select
    name,
    time, 
    front, 
    case when (front = 1 and (lag(front) over (order by time) = 0))
      or (front = 0 and (lag(front) over (order by time) = 1)) then time end as eff_from,
    case when (front = 1 and (lead(front) over (order by time) = 0))
      or (front = 0 and (lead(front) over (order by time) = 1)) then time end as eff_to
    from 
    nord_journal 
    where name = 'V212_open' 

     -- and $__timeFilter(time)
    order by time
      ) t
      )tt
  WHERE eff_from is not null 
      and front = 1 
) v
JOIN
nord_1s s
ON s.time between  v.eff_from and v.eff_to - interval '11 second' --последние 11 с сливаются на пол
    and s.Protok_CIP_2k > 8 
) ss
join
nord_alkali a
ON a.time between ss.time - interval '499 milliseconds'  and  ss.time + interval '499 milliseconds'
and (s.time between to_timestamp('2023-04-20 00:00:01', 'YYYY-MM-DD HH24:MI:SS.MS' ) 
and to_timestamp('2023-04-20 07:00:02', 'YYYY-MM-DD HH24:MI:SS.MS' ))

GROUP BY v.eff_from, v.eff_to
    );


