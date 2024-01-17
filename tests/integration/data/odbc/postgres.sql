--  #     Copyright 2022. ThingsBoard
--  #
--  #     Licensed under the Apache License, Version 2.0 (the "License");
--  #     you may not use this file except in compliance with the License.
--  #     You may obtain a copy of the License at
--  #
--  #         http://www.apache.org/licenses/LICENSE-2.0
--  #
--  #     Unless required by applicable law or agreed to in writing, software
--  #     distributed under the License is distributed on an "AS IS" BASIS,
--  #     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  #     See the License for the specific language governing permissions and
--  #     limitations under the License.

CREATE TABLE IF NOT EXISTS public.devices (
    id smallint,
    name character varying(32)
);

CREATE TABLE IF NOT EXISTS public.attributes (
    device_id smallint,
    ts bigint not null,
    key character varying(32),
    bool_v boolean,
    str_v character varying(32),
    long_v bigint,
    dbl_v double precision
);

CREATE TABLE IF NOT EXISTS public.timeseries (
    device_id smallint,
    ts bigint not null,
    bool_v boolean,
    str_v character varying(32),
    long_v bigint,
    dbl_v double precision
);

CREATE TABLE IF NOT EXISTS public.rpc (
    inc_value smallint not null,
    str_value character varying(32) not null,
    int_value smallint not null
);

CREATE OR REPLACE PROCEDURE public.increment_value()
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE public.rpc SET inc_value = inc_value + 1;
END
$$;

CREATE OR REPLACE FUNCTION public.decrement_value()
    RETURNS smallint
    LANGUAGE plpgsql
    AS $$
DECLARE
    value smallint;
BEGIN
    EXECUTE 'SELECT inc_value FROM public.rpc LIMIT 1' INTO value;
    value := value - 1;
    UPDATE public.rpc SET inc_value = value;
    RETURN value;
END
$$;

CREATE OR REPLACE FUNCTION public.reset_values(string_value character varying, integer_value smallint)
    RETURNS smallint
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE public.rpc SET str_value = string_value, int_value = integer_value;
    RETURN 1;
END
$$;

CREATE OR REPLACE FUNCTION public.secret_function(string_value character varying)
    RETURNS smallint
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE public.rpc SET str_value = string_value, inc_value = inc_value + 1;
    RETURN 1;
END
$$;

CREATE OR REPLACE PROCEDURE public.update_values(string_value character varying, integer_value smallint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE public.rpc SET str_value = string_value, int_value = integer_value;
END
$$;

CREATE OR REPLACE FUNCTION public.get_integer_value(column_name character varying, OUT return_value smallint)
    RETURNS smallint
    LANGUAGE plpgsql
    AS $$
BEGIN
    EXECUTE format('SELECT %s FROM public.rpc', column_name) into return_value;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_string_value(column_name character varying)
    RETURNS character varying
    LANGUAGE plpgsql
    AS $$
DECLARE
    return_value character varying;
BEGIN
    EXECUTE format('SELECT %s FROM public.rpc', column_name) into return_value;
    RETURN return_value;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_values()
    RETURNS SETOF public.rpc
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY SELECT * FROM public.rpc LIMIT 1;
END;
$$;

INSERT INTO public.rpc VALUES (1, 'test', 25);

INSERT INTO public.devices (name) VALUES ('Sensor 1');
INSERT INTO public.devices (name) VALUES ('Sensor 2');
INSERT INTO public.devices (name) VALUES ('Sensor 3');
INSERT INTO public.devices (name) VALUES ('Sensor 4');

INSERT INTO public.timeseries (device_id, ts, bool_v, str_v, long_v, dbl_v) VALUES (1, 1589476731000, NULL, NULL, 0, NULL);
INSERT INTO public.timeseries (device_id, ts, bool_v, str_v, long_v, dbl_v) VALUES (1, 1589476732000, NULL, NULL, 0, NULL);
INSERT INTO public.timeseries (device_id, ts, bool_v, str_v, long_v, dbl_v) VALUES (1, 1589476733000, NULL, NULL, 5, NULL);
INSERT INTO public.timeseries (device_id, ts, bool_v, str_v, long_v, dbl_v) VALUES (1, 1589476734000, NULL, NULL, 5, NULL);
INSERT INTO public.timeseries (device_id, ts, bool_v, str_v, long_v, dbl_v) VALUES (1, 1589476735000, NULL, NULL, 9, NULL);
INSERT INTO public.timeseries (device_id, ts, bool_v, str_v, long_v, dbl_v) VALUES (1, 1589476736000, NULL, NULL, 9, NULL);

INSERT INTO public.attributes (device_id, ts, key, bool_v, str_v, long_v, dbl_v) VALUES (2, 1589476731000, 'serialNumber', NULL, '123456789', NULL, NULL);
INSERT INTO public.attributes (device_id, ts, key, bool_v, str_v, long_v, dbl_v) VALUES (2, 1589476732000, 'enableUpgrade', True, NULL, NULL, NULL);
INSERT INTO public.attributes (device_id, ts, key, bool_v, str_v, long_v, dbl_v) VALUES (2, 1589476733000, 'latitude', NULL, NULL, NULL, 51.62);
INSERT INTO public.attributes (device_id, ts, key, bool_v, str_v, long_v, dbl_v) VALUES (2, 1589476734000, 'longitude', NULL, NULL, NULL, 35.63);
INSERT INTO public.attributes (device_id, ts, key, bool_v, str_v, long_v, dbl_v) VALUES (2, 1589476735000, 'softwareVersion', NULL, NULL, 465, NULL);
