CREATE EXTENSION IF NOT EXISTS pgcrypto;


CREATE TABLE public.contacts (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE),
	first_name varchar(64) NULL,
	last_name varchar(64) NULL,
	phone varchar(15) NULL,
	"extension" varchar(4) NULL,
	email varchar(254) NULL,
	subaddress varchar(64) NULL,
	phone_hash bytea NULL GENERATED ALWAYS AS (digest(COALESCE(phone, ''::character varying)::text, 'sha256'::text)) STORED,
	email_hash bytea NULL GENERATED ALWAYS AS (digest(lower(COALESCE(email, ''::character varying)::text), 'sha256'::text)) STORED,
	full_email_hash bytea NULL GENERATED ALWAYS AS (digest(lower((COALESCE(email, ''::character varying)::text || '+'::text) || COALESCE(subaddress, ''::character varying)::text), 'sha256'::text)) STORED,
	CONSTRAINT contacts_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_email_hash ON public.contacts USING btree (email_hash);
CREATE INDEX idx_full_email_hash ON public.contacts USING btree (full_email_hash);
CREATE INDEX idx_phone_hash ON public.contacts USING btree (phone_hash);