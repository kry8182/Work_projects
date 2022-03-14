select 
		(avg_today.t3 - avg_2021.t3) t3,
		(avg_today.t1 - avg_2021.t1) t1,
		(avg_today.t7 - avg_2021.t7) t7,
		(avg_today.t6 - avg_2021.t6) t6,
		(avg_today.t14 - avg_2021.t14) t14,
		(avg_today.t13 - avg_2021.t13) t13,
		(avg_today.t4 - avg_2021.t4) t4,
		(avg_today.t16 - avg_2021.t16) t16,
		(avg_today.t12 - avg_2021.t12) t12,
		(avg_today.t18 - avg_2021.t18) t18,
		(avg_today.t17 - avg_2021.t17) t17,
		(avg_today.t19 - avg_2021.t19) t19,
		(avg_today.t21 - avg_2021.t21) t21,
		(avg_today.t15 - avg_2021.t15) t15,
		(avg_today.t20 - avg_2021.t20) t20
from
AVERAGE2021_TEMP_STERIL_CREAM avg_2021
join (
select * from
(select 
cast(avg(t_tank03) as numeric(5,2)) t3
from (select t_tank03 from table1 where t_tank03 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
     ) tab3) tt3
inner join (
select 
cast(avg(t_tank01) as numeric(5,2)) t1
from (select t_tank01 from table1 where t_tank01 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab1
			) tt1
on 1=1
inner join (
select 
cast(avg(t_tank07) as numeric(5,2)) t7
from (select t_tank07 from table1 where t_tank07 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab7
			) tt7
on 1=1
inner join (
select 
cast(avg(t_tank06) as numeric(5,2)) t6
from (select t_tank06 from table1 where t_tank06 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab6
			) tt6
on 1=1
inner join (
select 
cast(avg(t_tank14) as numeric(5,2)) t14
from (select t_tank14 from table1 where t_tank14 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab14
			) tt14
on 1=1
inner join (
select 
cast(avg(t_tank13) as numeric(5,2)) t13
from (select t_tank13 from table1 where t_tank13 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab13
			) tt13
on 1=1
inner join (
select 
cast(avg(t_tank04) as numeric(5,2)) t4
from (select t_tank04 from table1 where t_tank04 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab4
			) tt4
on 1=1
inner join (
select 
cast(avg(t_tank16) as numeric(5,2)) t16
from (select t_tank16 from table1 where t_tank16 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab16
			) tt16
on 1=1
inner join (
select 
cast(avg(t_tank12) as numeric(5,2)) t12
from (select t_tank12 from table1 where t_tank12 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab12
			) tt12
on 1=1
inner join (
select 
cast(avg(t_tank18) as numeric(5,2)) t18
from (select t_tank18 from table1 where t_tank18 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab18
			) tt18
on 1=1
inner join (
select 
cast(avg(t_tank17) as numeric(5,2)) t17
from (select t_tank17 from table1 where t_tank17 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab17
			) tt17
on 1=1
inner join (
select 
cast(avg(t_tank19) as numeric(5,2)) t19
from (select t_tank19 from table1 where t_tank19 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab19
			) tt19
on 1=1
inner join (
select 
cast(avg(t_tank21) as numeric(5,2)) t21
from (select t_tank21 from table1 where t_tank21 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab21
			) tt21
on 1=1
inner join (
select 
cast(avg(t_tank15) as numeric(5,2)) t15
from (select t_tank15 from table1 where t_tank15 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab15
			) tt15
on 1=1
inner join (
select 
cast(avg(t_tank20) as numeric(5,2)) t20
from (select t_tank20 from table1 where t_tank20 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab20
			) tt20
on 1=1
	) avg_today
on 1=1

UNION ALL

select * from
(select 
cast(avg(t_tank03) as numeric(5,2)) t3
from (select t_tank03 from table1 where t_tank03 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
     ) tab3) tt3
inner join (
select 
cast(avg(t_tank01) as numeric(5,2)) t1
from (select t_tank01 from table1 where t_tank01 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab1
			) tt1
on 1=1
inner join (
select 
cast(avg(t_tank07) as numeric(5,2)) t7
from (select t_tank07 from table1 where t_tank07 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab7
			) tt7
on 1=1
inner join (
select 
cast(avg(t_tank06) as numeric(5,2)) t6
from (select t_tank06 from table1 where t_tank06 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab6
			) tt6
on 1=1
inner join (
select 
cast(avg(t_tank14) as numeric(5,2)) t14
from (select t_tank14 from table1 where t_tank14 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab14
			) tt14
on 1=1
inner join (
select 
cast(avg(t_tank13) as numeric(5,2)) t13
from (select t_tank13 from table1 where t_tank13 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab13
			) tt13
on 1=1
inner join (
select 
cast(avg(t_tank04) as numeric(5,2)) t4
from (select t_tank04 from table1 where t_tank04 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab4
			) tt4
on 1=1
inner join (
select 
cast(avg(t_tank16) as numeric(5,2)) t16
from (select t_tank16 from table1 where t_tank16 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab16
			) tt16
on 1=1
inner join (
select 
cast(avg(t_tank12) as numeric(5,2)) t12
from (select t_tank12 from table1 where t_tank12 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab12
			) tt12
on 1=1
inner join (
select 
cast(avg(t_tank18) as numeric(5,2)) t18
from (select t_tank18 from table1 where t_tank18 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab18
			) tt18
on 1=1
inner join (
select 
cast(avg(t_tank17) as numeric(5,2)) t17
from (select t_tank17 from table1 where t_tank17 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab17
			) tt17
on 1=1
inner join (
select 
cast(avg(t_tank19) as numeric(5,2)) t19
from (select t_tank19 from table1 where t_tank19 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab19
			) tt19
on 1=1
inner join (
select 
cast(avg(t_tank21) as numeric(5,2)) t21
from (select t_tank21 from table1 where t_tank21 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab21
			) tt21
on 1=1
inner join (
select 
cast(avg(t_tank15) as numeric(5,2)) t15
from (select t_tank15 from table1 where t_tank15 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab15
			) tt15
on 1=1
inner join (
select 
cast(avg(t_tank20) as numeric(5,2)) t20
from (select t_tank20 from table1 where t_tank20 > 80 
and table1.current_time between current_date and (current_date + interval '1' day)
	  ) tab20
			) tt20
on 1=1

UNION ALL

SELECT * FROM AVERAGE2021_TEMP_STERIL_CREAM;
