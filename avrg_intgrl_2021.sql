
-- Не доделано, только начато

select * from 
(select 
	avg(t)*count(t)*10 as t3_integral
from (select 
		t_tank03 as t
	   from table1 where t_tank03 > 85
	   and table1.current_time between (timestamp '{date}' - interval '1' day)  and timestamp '{date}'  
      ) tab

)tt3
cross join 
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


	t3,
    t1,
    t7,
    t6,
    t14,
    t13,
    t4,
    t16,
    t12,
    t18,
    t17,
    t19,
    t21,
    t15,
    t20
)
