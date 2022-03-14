-- Table: public.dieta_alkali_wash_stg

-- DROP TABLE public.dieta_alkali_wash_stg;

CREATE TABLE public.dieta_alkali_wash_stg
(
    "time" timestamp without time zone NOT NULL,
    number_wash integer,
    conc integer,
    CONSTRAINT dieta_alkali_wash_stg_pkey PRIMARY KEY ("time")
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.dieta_alkali_wash_stg
    OWNER to kip;

-- Table: public.dieta_alkali_bak

-- DROP TABLE public.dieta_alkali_bak;

CREATE TABLE public.dieta_alkali_bak
(
    "time" timestamp without time zone NOT NULL,
    conc real,
    CONSTRAINT dieta_alkali_bak_pkey PRIMARY KEY ("time")
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.dieta_alkali_bak
    OWNER to kip;