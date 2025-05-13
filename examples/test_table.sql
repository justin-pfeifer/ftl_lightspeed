CREATE TABLE public.contacts (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE),
	first_name varchar(64) NULL,
	last_name varchar(64) NULL,
	phone varchar(15) NULL,
	"extension" varchar(4) NULL,
	email varchar(254) NULL,
	subaddress varchar(64) NULL,
	CONSTRAINT contacts_pkey PRIMARY KEY (id)
);