    temp_acid real,
    temp_alk real,
    temp_gv real,
    temp_vozvr real,
    temp_vozvr_2k real,
    conc_acid real,
    conc_alk real,
    conc_gv real,
    conc_vozvr real,
    conc_vozvr_2k real,
    alarm_bit boolean,
    alarm_bit_2k boolean,
    bit_sirena boolean,
    bit_sirena_2k boolean,
    no_potok boolean,
    no_potok_2k boolean,
    no_vozvr boolean,
    no_vozvr_2k boolean,
    pvozd_mc boolean,
    pvozd_tank boolean,
    alarm_button boolean,
    exceeding_time_otkachki boolean,
    sboy_marshruta boolean,
    marshrut_ne_sobralsa boolean,
    exceeding_time_otkachki_2k boolean,
    sboy_marshruta_2k boolean,
    marshrut_ne_sobralsa_2k boolean,
    ban_coldwater boolean,
    ban_hotwater boolean,
    alarm34 smallint,
    alarm35 smallint,
    alarm36 smallint,
    alarm37 smallint,
    alarm34_2k smallint,
    alarm35_2k smallint,
    alarm36_2k smallint,
    alarm37_2k smallint,
    pressure real,
    pressure_2k real,
    potok_cip real,
    potok_cip_2k real,
    protok_vozr real,
    protok_vozr_2k real
) 
    --CONSTRAINT table_name_pk PRIMARY KEY (id)

WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.nord
    OWNER to kip;
_1s

ALTER TABLE public.nord_1s
    
ADD COLUMN Temperature_CIP real ,  
ADD COLUMN Temperature_na_vozvrate real ,  
ADD COLUMN Alarm_bit  boolean  ,  
ADD COLUMN Pressure_CIP real,  
ADD COLUMN Potok  real  ,  
ADD COLUMN Protok_CIP  real ,  
ADD COLUMN Temperature_CIP_2k real   ,  
ADD COLUMN Pressure_CIP_2k  real,
ADD COLUMN Alarm_bit_2k boolean,  
ADD COLUMN Protok_CIP_2k  real  ,  
ADD COLUMN Potok_2k real,
ADD COLUMN Show_0 boolean,
ADD COLUMN Show_0_2k boolean;

ALTER TABLE public.nord_1min
    
ADD COLUMN Temperature_CIP real ,  
ADD COLUMN Temperature_na_vozvrate real ,  
ADD COLUMN Alarm_bit  boolean  ,  
ADD COLUMN Pressure_CIP real,  
ADD COLUMN Potok  real  ,  
ADD COLUMN Protok_CIP  real ,  
ADD COLUMN Temperature_CIP_2k real   , 
ADD COLUMN Temperature_na_vozvrate_2k real , 
ADD COLUMN Pressure_CIP_2k  real,
ADD COLUMN Alarm_bit_2k boolean,  
ADD COLUMN Protok_CIP_2k  real  ,  
ADD COLUMN Potok_2k real,
ADD COLUMN Show_0 boolean,
ADD COLUMN Show_0_2k boolean,
ADD COLUMN Temperature_HotWoter real,
ADD COLUMN Concentration_Alk real , 
ADD COLUMN Concentration_Acid real, 
ADD COLUMN Concentration_vozvr real, 
ADD COLUMN Concentration_vozvr_2k real,
ADD COLUMN Approx_val_T1 real;

-- Или добавляем столбцы к созданной в pg_admin таблице только с первыми двумя столбцами (чтобы ее было видно в дереве)
ALTER TABLE public.nord_1s
    
ADD COLUMN Temperature_CIP real ,  
ADD COLUMN Temperature_na_vozvrate real ,  
ADD COLUMN Alarm_bit  boolean  ,  
ADD COLUMN Pressure_CIP real,  
ADD COLUMN Potok  real  ,  
ADD COLUMN Protok_CIP  real ,  
ADD COLUMN Temperature_CIP_2k real   ,
ADD COLUMN Temperature_na_vozvrate_2k  real ,  
ADD COLUMN Pressure_CIP_2k  real,
ADD COLUMN Alarm_bit_2k boolean,  
ADD COLUMN Protok_CIP_2k  real  ,  
ADD COLUMN Potok_2k real,
ADD COLUMN Show_0 boolean,
ADD COLUMN Show_0_2k boolean;

ALTER TABLE public.nord_1h

ADD COLUMN Temperature_Acid real,
ADD COLUMN Temperature_Alcali real,
ADD COLUMN Concentration_gv real;

ALTER TABLE public.nord_alkali
ADD COLUMN Concentration_Alk real, 
ADD COLUMN h_urov_sheloch	boolean,
ADD COLUMN v112_open boolean,
ADD COLUMN v212_open boolean,
ADD COLUMN v114_open	boolean, 
ADD COLUMN Temperature_Alcali	real,
ADD COLUMN V107 boolean	,
ADD COLUMN alkali_not_ready	boolean,
ADD COLUMN Concentration_vozvr	real, 
ADD COLUMN Concentration_vozvr_2k real;

ALTER TABLE public.nord_alarm

ADD COLUMN Alarm_bit	boolean, 
ADD COLUMN Alarm_bit_2k boolean, 
ADD COLUMN bit_sirena boolean	,
ADD COLUMN bit_sirena_2k boolean	,
ADD COLUMN alarm34	integer, 
ADD COLUMN alarm35 integer	,
ADD COLUMN alarm36 integer	,
ADD COLUMN alarm37	integer,
ADD COLUMN no_potok boolean	,
ADD COLUMN no_vozvrat boolean	,
ADD COLUMN Pvozd_MC boolean	,
ADD COLUMN Pvozd_tank boolean	,
ADD COLUMN avar_button	boolean,
ADD COLUMN Exceeding_time_otkachki boolean	,
ADD COLUMN Sboy_marshruta boolean	,
ADD COLUMN Marshrut_ne_sobralsa boolean	,
ADD COLUMN ban_coldwater boolean	,
ADD COLUMN ban_hotwater boolean	,
ADD COLUMN alarm34_2k	integer,
ADD COLUMN alarm35_2k	integer,
ADD COLUMN alarm36_2k integer	,
ADD COLUMN alarm37_2k integer	,
ADD COLUMN no_potok_2k boolean	,
ADD COLUMN no_vozvrat_2k boolean,
ADD COLUMN Exceeding_time_otkachki_2k boolean	,
ADD COLUMN Sboy_marshruta_2k boolean	,
ADD COLUMN Marshrut_ne_sobralsa_2k boolean

ALTER TABLE public.nord_other

ADD COLUMN Out_Current_potok_2k	real;

ALTER TABLE public.nord_journal
  ADD COLUMN name text,
  ADD COLUMN level text,
  ADD COLUMN front integer;





ALTER TABLE public.nord

   ADD COLUMN  temp_acid real,
   ADD COLUMN  temp_alk real,
   ADD COLUMN temp_gv real,
   ADD COLUMN  temp_cip real,
   ADD COLUMN  temp_cip_2k real,
   ADD COLUMN  temp_vozvr real,
   ADD COLUMN  temp_vozvr_2k real,
   ADD COLUMN  conc_acid real,
   ADD COLUMN  conc_alk real,
   ADD COLUMN  conc_gv real,
   ADD COLUMN  conc_vozvr real,
   ADD COLUMN  conc_vozvr_2k real,
   ADD COLUMN  alarm_bit boolean,
   ADD COLUMN  alarm_bit_2k boolean,
   ADD COLUMN  bit_sirena boolean,
   ADD COLUMN  bit_sirena_2k boolean,
   ADD COLUMN  no_potok boolean,
   ADD COLUMN  no_potok_2k boolean,
   ADD COLUMN  no_vozvr boolean,
   ADD COLUMN  no_vozvr_2k boolean,
   ADD COLUMN  pvozd_mc boolean,
   ADD COLUMN  pvozd_tank boolean,
   ADD COLUMN  alarm_button boolean,
   ADD COLUMN  exceeding_time_otkachki boolean,
   ADD COLUMN  sboy_marshruta boolean,
   ADD COLUMN marshrut_ne_sobralsa boolean,
   ADD COLUMN  exceeding_time_otkachki_2k boolean,
   ADD COLUMN  sboy_marshruta_2k boolean,
   ADD COLUMN  marshrut_ne_sobralsa_2k boolean,
   ADD COLUMN  ban_coldwater boolean,
   ADD COLUMN  ban_hotwater boolean,
   ADD COLUMN  alarm34 smallint,
   ADD COLUMN  alarm35 smallint,
   ADD COLUMN  alarm36 smallint,
   ADD COLUMN  alarm37 smallint,
   ADD COLUMN  alarm34_2k smallint,
   ADD COLUMN  alarm35_2k smallint,
   ADD COLUMN  alarm36_2k smallint,
   ADD COLUMN  alarm37_2k smallint,
   ADD COLUMN  pressure real,
   ADD COLUMN  pressure_2k real,
   ADD COLUMN  potok_cip real,
   ADD COLUMN  potok_cip_2k real,
   ADD COLUMN  protok_vozr real,
   ADD COLUMN  protok_vozr_2k real;

ALTER TABLE public.nord_analog_1s_STG
   ADD COLUMN  temp_cip real,
   ADD COLUMN  temp_cip real,
   ADD COLUMN  temp_vozvr real,
   ADD COLUMN  temp_vozvr_2k real,
   ADD COLUMN  alarm_bit boolean,
   ADD COLUMN  alarm_bit_2k boolean,
   ADD COLUMN  pressure real,
   ADD COLUMN  pressure_2k real,
   ADD COLUMN  potok_cip real,
   ADD COLUMN  potok_cip_2k real,
   ADD COLUMN  protok_vozr real,
   ADD COLUMN  protok_vozr_2k real;