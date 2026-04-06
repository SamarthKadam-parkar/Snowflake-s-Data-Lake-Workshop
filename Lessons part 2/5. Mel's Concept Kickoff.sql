use role sysadmin;

create database if not exists MELS_SMOOTHIE_CHALLENGE_DB;

DROP SCHEMA MELS_SMOOTHIE_CHALLENGE_DB.PUBLIC;

CREATE SCHEMA IF NOT EXISTS MELS_SMOOTHIE_CHALLENGE_DB.TRAILS;

SELECT $1 FROM @TRAILS_GEOJSON
(FILE_FORMAT => FF_JSON);

create view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHEERY_CREEK_TRAIL AS
(select 
     $1 : sequence_1 as point_id 
    ,$1 : trail_name :: varchar as trail_name
    ,$1 : latitude :: number(11,8) as latitude
    ,$1 : longitude :: number(11,8) as longitude

from @trails_parquet
(file_format => FF_PARQUET)
order by point_id);

--Using concatenate to prepare the data for plotting on a map
select top 100 
 longitude||' '||latitude as coord_pair
,'POINT('||coord_pair||')' as trail_point
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHEERY_CREEK_TRAIL;

--To add a column, we have to replace the entire view
--changes to the original are shown in red
create or replace view cherry_creek_trail as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;

SELECT $1 
FROM @TRAILS_GEOJSON
(FILE_FORMAT => FF_JSON);

CREATE VIEW MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS AS 
(select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json));


--Remember this code? 
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
,st_length(to_geography(my_linestring)) as length_of_trail --this line is new! but it won't work!
from cherry_creek_trail
group by trail_name;

CREATE VIEW MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.
(select * from denver_area_trails;
select 
     feature_name
    ,st_length(to_geography(GEOMETRY)) AS WO_LENGTH
    ,st_length(to_geography(GEOMETRY)) AS GEOM_LENGTH
FROM DENVER_AREA_TRAILS;)

select get_ddl('view', 'DENVER_AREA_TRAILS');


create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS(
	FEATURE_NAME,
	FEATURE_COORDINATES,
	GEOMETRY,
    TRAIL_LENGTH,
	FEATURE_PROPERTIES,
	SPECS,
	WHOLE_OBJECT
) as 
(select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,ST_LENGTH(TO_GEOGRAPHY(GEOMETRY)) AS TRAIL_LENGTH
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json));

SELECT * FROM DENVER_AREA_TRAILS;

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create or replace view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',') within group (order by point_id)||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry))  as trail_length
from cherry_creek_trail
group by trail_name;

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;


--Add more GeoSpatial Calculations to get more GeoSpecial Information! 
CREATE VIEW TRAILS_AND_BOUNDARIES AS
(select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2);

SELECT * FROM TRAILS_AND_BOUNDARIES;

select 'POLYGON(('|| 
    min(min_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||min(min_northsouth)||','|| 
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from TRAILS_AND_BOUNDARIES;