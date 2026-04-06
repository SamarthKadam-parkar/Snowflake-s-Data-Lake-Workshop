create or replace table util_db.public.my_data_types
(
  my_number number
, my_text varchar(10)
, my_bool boolean
, my_float float
, my_date date
, my_timestamp timestamp_tz
, my_variant variant
, my_array array
, my_object object
, my_geography geography
, my_geometry geometry
, my_vector vector(int,16)
);

describe table util_db.public.my_data_types;

USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS ZENAS_ATHLEISURE_DB;

DROP SCHEMA IF EXISTS ZENAS_ATHLEISURE_DB.PUBLIC;

CREATE SCHEMA IF NOT EXISTS ZENAS_ATHLEISURE_DB.PRODUCTS;