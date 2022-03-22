CREATE TABLE public.steril_integrals_on_date
(
    date date NOT NULL,
    t03 numeric,
    t01 numeric,
    t07 numeric,
    t06 numeric,
    t14 numeric,
    t13 numeric,
    t04 numeric,
    t16 numeric,
    t12 numeric,
    t18 numeric,
    t17 numeric,
    t19 numeric,
    t21 numeric,
    t15 numeric,
    t20 numeric,
    c03 integer,
    c01 integer,
    c07 integer,
    c06 integer,
    c14 integer,
    c13 integer,
    c04 integer,
    c16 integer,
    c12 integer,
    c18 integer,
    c17 integer,
    c19 integer,
    c21 integer,
    c15 integer,
    c20 integer,
    CONSTRAINT steril_integrals_on_date_pkey PRIMARY KEY (date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.steril_integrals_on_date
    OWNER to kip;