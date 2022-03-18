-- Перевод времени в секунды

select (EXTRACT(hour FROM table1.current_time)*60*60 
       + EXTRACT(minutes FROM table1.current_time)*60
       + EXTRACT(seconds FROM table1.current_time)) sec
from table1 
-- where table1.current_time > current_date - interval '100' second; 

-- Вычисление нечестного "интеграла" стерилизации для одного из танков
select 
cast(avg(t)*(max(h) - min(h)) as numeric(5,2)) as t3_integral
from (select 
		t_tank03 as t,
	   (EXTRACT(hour FROM table1.current_time)*60*60 
       + EXTRACT(minutes FROM table1.current_time)*60
       + EXTRACT(seconds FROM table1.current_time)) / (60*60) as h
	   from table1 where t_tank03 > 80 
	   and table1.current_time between (current_date - interval '1' day) and current_date
      ) tab

-- Вычисление  честного "интеграла" стерилизации  для одного из танков
select 
	avg(t)*count(t)*10 as t3_integral
from (select 
		t_tank03 as t
	   from table1 where t_tank03 > 85
	   and table1.current_time between (timestamp '2022/03/14' - interval '1' day)  and (timestamp '2022/03/14')  
      ) tab
-- и вставка его в таблицу
insert into steril_integals_on_date( date, t3 ) 
    select 
        timestamp '2022/03/14',
        avg(t)*count(t)*10 as t3_integral
    from (select 
        t_tank03 as t
       from table1 where t_tank03 > 80
       and table1.current_time between (timestamp '2022/03/14' - interval '1' day)  and timestamp '2022/03/14'  
         ) tab
 
--Среднее и максимум за период 
select cast(max(t3) as numeric(7,0)), cast(avg(t3)as numeric(7,0)) from steril_integals_on_date
where  date between (timestamp '2022/02/14'  and timestamp '2022/03/14' ;


--Нахождение даты максимума интеграла стерилизации в 2021 году
select date, max from 
(select cast(max(t3) as numeric(7,0)) max, cast(avg(t3)as numeric(7,0)) from steril_integals_on_date s
where  date between (timestamp '2021/01/01' - interval '1' day)  and timestamp '2022/01/01') tab
join steril_integals_on_date s
on cast(t3 as numeric(7,0)) = tab.max

-- Вычисление отношения честного "интеграла" стерилизации к идеальному максимуму (90 град в течение 15 минут) для одного из танков
select 
	cast(avg(t)*count(t)*10/(90*900) as numeric(5,2)) as t3_integral
from (select 
		t_tank03 as t
	   from table1 where t_tank03 > 85
	   and table1.current_time between (timestamp '2022/03/14' - interval '1' day) and current_date
      ) tab



-- Вычисление нечестного "интеграла" стерилизации для двух танков
select * from
(select 
cast(avg(t)*(max(h) - min(h)) as numeric(5,2)) as t3_integral
from (select 
		t_tank03 as t,
	   (EXTRACT(hour FROM table1.current_time)*60*60 
       + EXTRACT(minutes FROM table1.current_time)*60
       + EXTRACT(seconds FROM table1.current_time)) / (60*60) as h
	   from table1 where t_tank03 > 80 
	   and table1.current_time between (current_date - interval '1' day) and current_date
      ) tab
)tt3
join 
(select 
cast(avg(t)*(max(h) - min(h)) as numeric(5,2)) as t1_integral
from (select 
		t_tank01 as t,
	   (EXTRACT(hour FROM table1.current_time)*60*60 
       + EXTRACT(minutes FROM table1.current_time)*60
       + EXTRACT(seconds FROM table1.current_time)) / (60*60) as h
	   from table1 where t_tank01 > 80 
	   and table1.current_time between (current_date - interval '1' day) and current_date
      ) tab
)tt1
on 1=1