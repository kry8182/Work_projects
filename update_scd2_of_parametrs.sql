-- UPDATE scd2  ТАБЛИЦЫ ПАРАМЕТРОВ (в три шага, за неимением MERGE в postgres) :

-- 1) вставляем строку свежих параметров
insert into nord_parametrs
(
select
d.time
,null
,d.Dpid_potok_LINT
,d.Dpid_potok_LINT_2k
,d.Dpid_temp_LINT
,d.Dpid_temp_LINT_2k
,d.Ipid_potok_LINT
,d.Ipid_potok_LINT_2k
,d.Ipid_temp_LINT
,d.Ipid_temp_LINT_2k
,d.Porog_ostatka_v_tanke
,d.Porog_protoka
,d.Porog_protoka_2k
,d.porog_vozvrata_acid
,d.porog_vozvrata_acid_2k
,d.porog_vozvrata_alkali
,d.basic_porog_vozvrata_alkali_2k
,d.porog_vozvrata_alkali_adapt_2k
,d.porog_vozvrata_alkali_priemka_2k
,d.Ppid_potok
,d.Ppid_potok_2k
,d.Ppid_temp
,d.Ppid_temp_2k
,d.set_concentr_Acid
,d.set_concentr_alkali
,d.Temperature_Nagrev_HotWoter_SP
,d.Time_1_doz_acid_LINT
,d.Time_1_doz_alkali_LINT
,d.Time_2_doz_acid_LINT
,d.Time_2_doz_alkali_LINT
,d.Time_acid_line
,d.Time_acid_line_2k
,d.Time_acid_tank
,d.Time_acid_tank_2k
,d.Time_alkali_line
,d.Time_alkali_line_2k
,d.Time_alkali_tank
,d.Time_alkali_tank_2k
,d.Time_cold_water_line
,d.Time_cold_water_line_2k
,d.Time_cold_water_tank
,d.Time_cold_water_tank_2k
,d.Time_cooling_line
,d.Time_cooling_line_2k
,d.Time_cooling_tank
,d.Time_cooling_tank_2k
,d.Time_del_M12off_line_LINT_2k
,d.Time_del_M12off_tank_LINT_2k
,d.Time_del_M12on_line_LINT_2k
,d.Time_del_M12on_tank_LINT_2k
,d.Time_del_M2off_line_LINT
,d.Time_del_M2off_tank_LINT
,d.Time_del_M2on_line_LINT
,d.Time_del_M2on_tank_LINT
,d.Time_drivers_LINT
,d.Time_flowsens_LINT
,d.Time_flowsens_LINT_2k
,d.Time_hot_water_line
,d.Time_hot_water_line_2k
,d.Time_hot_water_tank
,d.Time_hot_water_tank_2k
,d.Time_man_peremesh_LINT
,d.Time_measur_cook_acid_LINT
,d.Time_measur_cook_alkali_LINT
,d.Time_mix_cook_acid_LINT
,d.Time_mix_cook_alkali_LINT
,d.Time_nagreva_baka_Hot_Water
,d.Time_nagreva_bakov
,d.Time_opolask_do_alkali_line
,d.Time_opolask_do_alkali_line_2k
,d.Time_opolask_do_alkali_tank
,d.Time_opolask_do_alkali_tank_2k
,d.Time_opolask_posle_acid_line
,d.Time_opolask_posle_acid_line_2k
,d.Time_opolask_posle_acid_tank
,d.Time_opolask_posle_acid_tank_2k
,d.Time_opolask_posle_alkali_line
,d.Time_opolask_posle_alkali_line_2k
,d.Time_opolask_posle_alkali_tank
,d.Time_opolask_posle_alkali_tank_2k
,d.Time_promyv_posle_nagreva_LINT
,d.Time_protalk
,d.Time_protalk_2k
,d.Time_steril_line_2k
,d.Time_steril_tank_2k
,d.Time_TON_M1_V1111_LINT
,d.Time_TON_M1_V2111_LINT_2k
,d.Time_valves_LINT
,d.Time_vozvrat_line_LINT
,d.Time_vozvrat_line_LINT_2k
,d.Time_vozvrat_tank_LINT
,d.Time_vozvrat_tank_LINT_2k
,d.Time_vozvrat_tank_Syvorotki_LINT
,d.ustavka_potok_2k
,d.ustavka_potok_L_Priemki_2k
,d.ustavka_potok_Steril_2k
,d.ustavka_potok_Steril_L_Priemki_2k
,d.ustavka1_temper
,d.ustavka1_temper_2k
,d.ustavka2_temper
,d.ustavka2_temper_2k
,d.ustavka3_temper
,d.ustavka3_temper_2k
,d.ustavka4_temper_2k
,d.zad_n_moek_acid
,d.zad_n_moek_alkali
from 
nord_parametrs p
join
nord_1d d
on 
p.eff_to = to_timestamp('2099-01-01', 'YYYY-MM-DD' )
and (d.time between p.eff_from and p.eff_to)
and (
p.Dpid_potok_LINT <> d.Dpid_potok_LINT
or p.Dpid_potok_LINT_2k <> d.Dpid_potok_LINT_2k
or p.Dpid_temp_LINT <>d.Dpid_temp_LINT
or p.Dpid_temp_LINT_2k  <>d.Dpid_temp_LINT_2k
or p.Ipid_potok_LINT <>d.Ipid_potok_LINT
or p.Ipid_potok_LINT_2k <>d.Ipid_potok_LINT_2k
or p.Ipid_temp_LINT <>d.Ipid_temp_LINT
or p.Ipid_temp_LINT_2k <>d.Ipid_temp_LINT_2k
or p.Porog_ostatka_v_tanke <>d.Porog_ostatka_v_tanke
or p.Porog_protoka <>d.Porog_protoka
or p.Porog_protoka_2k <>d.Porog_protoka_2k
or p.porog_vozvrata_acid <>d.porog_vozvrata_acid
or p.porog_vozvrata_acid_2k <>d.porog_vozvrata_acid_2k
or p.porog_vozvrata_alkali <>d.porog_vozvrata_alkali
or p.basic_porog_vozvrata_alkali_2k <>d.basic_porog_vozvrata_alkali_2k
or p.porog_vozvrata_alkali_adapt_2k <>d.porog_vozvrata_alkali_adapt_2k
or p.porog_vozvrata_alkali_priemka_2k <>d.porog_vozvrata_alkali_priemka_2k
or p.Ppid_potok <>d.Ppid_potok
or p.Ppid_potok_2k <>d.Ppid_potok_2k
or p.Ppid_temp <>d.Ppid_temp
or p.Ppid_temp_2k <>d.Ppid_temp_2k
or p.set_concentr_Acid <>d.set_concentr_Acid
or p.set_concentr_alkali <>d.set_concentr_alkali
or p.Temperature_Nagrev_HotWoter_SP <>d.Temperature_Nagrev_HotWoter_SP 
or p.Time_1_doz_acid_LINT <>d.Time_1_doz_acid_LINT
or p.Time_1_doz_alkali_LINT <>d.Time_1_doz_alkali_LINT 
or p.Time_2_doz_acid_LINT <>d.Time_2_doz_acid_LINT
or p.Time_2_doz_alkali_LINT <>d.Time_2_doz_alkali_LINT
or p.Time_acid_line <>d.Time_acid_line
or p.Time_acid_line_2k <>d.Time_acid_line_2k
or p.Time_acid_tank <>d.Time_acid_tank
or p.Time_acid_tank_2k <>d.Time_acid_tank_2k
or p.Time_alkali_line <>d.Time_alkali_line
or p.Time_alkali_line_2k <>d.Time_alkali_line_2k
or p.Time_alkali_tank <>d.Time_alkali_tank
or p.Time_alkali_tank_2k <>d.Time_alkali_tank_2k
or p.Time_cold_water_line <>d.Time_cold_water_line
or p.Time_cold_water_line_2k <>d.Time_cold_water_line_2k
or p.Time_cold_water_tank <>d.Time_cold_water_tank
or p.Time_cold_water_tank_2k <>d.Time_cold_water_tank_2k
or p.Time_cooling_line <>d.Time_cooling_line
or p.Time_cooling_line_2k <>d.Time_cooling_line_2k
or p.Time_cooling_tank <>d.Time_cooling_tank
or p.Time_cooling_tank_2k <>d.Time_cooling_tank_2k
or p.Time_del_M12off_line_LINT_2k <>d.Time_del_M12off_line_LINT_2k
or p.Time_del_M12off_tank_LINT_2k <>d.Time_del_M12off_tank_LINT_2k
or p.Time_del_M12on_line_LINT_2k <>d.Time_del_M12on_line_LINT_2k 
or p.Time_del_M12on_tank_LINT_2k <>d.Time_del_M12on_tank_LINT_2k
or p.Time_del_M2off_line_LINT <>d.Time_del_M2off_line_LINT
or p.Time_del_M2off_tank_LINT <>d.Time_del_M2off_tank_LINT 
or p.Time_del_M2on_line_LINT <>d.Time_del_M2on_line_LINT
or p.Time_del_M2on_tank_LINT <>d.Time_del_M2on_tank_LINT
or p.Time_drivers_LINT <>d.Time_drivers_LINT
or p.Time_flowsens_LINT <>d.Time_flowsens_LINT
or p.Time_flowsens_LINT_2k <>d.Time_flowsens_LINT_2k
or p.Time_hot_water_line <>d.Time_hot_water_line
or p.Time_hot_water_line_2k <>d.Time_hot_water_line_2k
or p.Time_hot_water_tank <>d.Time_hot_water_tank
or p.Time_hot_water_tank_2k <>d.Time_hot_water_tank_2k
or p.Time_man_peremesh_LINT <>d.Time_man_peremesh_LINT
or p.Time_measur_cook_acid_LINT <>d.Time_measur_cook_acid_LINT
or p.Time_measur_cook_alkali_LINT <>d.Time_measur_cook_alkali_LINT
or p.Time_mix_cook_acid_LINT <>d.Time_mix_cook_acid_LINT 
or p.Time_mix_cook_alkali_LINT <>d.Time_mix_cook_alkali_LINT 
or p.Time_nagreva_baka_Hot_Water <>d.Time_nagreva_baka_Hot_Water
or p.Time_nagreva_bakov <>d.Time_nagreva_bakov
or p.Time_opolask_do_alkali_line <>d.Time_opolask_do_alkali_line
or p.Time_opolask_do_alkali_line_2k <>d.Time_opolask_do_alkali_line_2k
or p.Time_opolask_do_alkali_tank <>d.Time_opolask_do_alkali_tank 
or p.Time_opolask_do_alkali_tank_2k <>d.Time_opolask_do_alkali_tank_2k
or p.Time_opolask_posle_acid_line <>d.Time_opolask_posle_acid_line 
or p.Time_opolask_posle_acid_line_2k <>d.Time_opolask_posle_acid_line_2k
or p.Time_opolask_posle_acid_tank <>d.Time_opolask_posle_acid_tank 
or p.Time_opolask_posle_acid_tank_2k <>d.Time_opolask_posle_acid_tank_2k
or p.Time_opolask_posle_alkali_line <>d.Time_opolask_posle_alkali_line
or p.Time_opolask_posle_alkali_line_2k <>d.Time_opolask_posle_alkali_line_2k
or p.Time_opolask_posle_alkali_tank <>d.Time_opolask_posle_alkali_tank 
or p.Time_opolask_posle_alkali_tank_2k <>d.Time_opolask_posle_alkali_tank_2k 
or p.Time_promyv_posle_nagreva_LINT <>d.Time_promyv_posle_nagreva_LINT
or p.Time_protalk <>d.Time_protalk 
or p.Time_protalk_2k <>d.Time_protalk_2k
or p.Time_steril_line_2k <>d.Time_steril_line_2k
or p.Time_steril_tank_2k <>d.Time_steril_tank_2k
or p.Time_TON_M1_V1111_LINT <>d.Time_TON_M1_V1111_LINT
or p.Time_TON_M1_V2111_LINT_2k <>d.Time_TON_M1_V2111_LINT_2k
or p.Time_valves_LINT <>d.Time_valves_LINT
or p.Time_vozvrat_line_LINT <>d.Time_vozvrat_line_LINT
or p.Time_vozvrat_line_LINT_2k <>d.Time_vozvrat_line_LINT_2k
or p.Time_vozvrat_tank_LINT <>d.Time_vozvrat_tank_LINT
or p.Time_vozvrat_tank_LINT_2k <>d.Time_vozvrat_tank_LINT_2k
or p.Time_vozvrat_tank_Syvorotki_LINT <>d.Time_vozvrat_tank_Syvorotki_LINT
or p.ustavka_potok_2k <>d.ustavka_potok_2k
or p.ustavka_potok_L_Priemki_2k <>d.ustavka_potok_L_Priemki_2k
or p.ustavka_potok_Steril_2k <>d.ustavka_potok_Steril_2k
or p.ustavka_potok_Steril_L_Priemki_2k <>d.ustavka_potok_Steril_L_Priemki_2k
or p.ustavka1_temper <>d.ustavka1_temper
or p.ustavka1_temper_2k <>d.ustavka1_temper_2k
or p.ustavka2_temper <>d.ustavka2_temper
or p.ustavka2_temper_2k <>d.ustavka2_temper_2k
or p.ustavka3_temper <>d.ustavka3_temper 
or p.ustavka3_temper_2k <>d.ustavka3_temper_2k
or p.ustavka4_temper_2k <>d.ustavka4_temper_2k
or p.zad_n_moek_acid <>d.zad_n_moek_acid
or p.zad_n_moek_alkali <>d.zad_n_moek_alkali
  )
);

-- 2) закрываем предыдущую версию:
update nord_parametrs
set eff_to = (select max(eff_from) from nord_parametrs where eff_to is null) - interval '1 second'
where eff_to = to_timestamp('2099-01-01', 'YYYY-MM-DD' ) 
and (select max(eff_from) from nord_parametrs where eff_to is null) is not null;

--3) вписываем бесконечный конец в eff_to последней версииЖ
update nord_parametrs
set eff_to = to_timestamp('2099-01-01', 'YYYY-MM-DD' )
where eff_to is null ;

--опустошаем STG:
truncate nord_1d;