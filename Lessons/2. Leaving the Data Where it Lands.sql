list @product_metadata

select $1 from @product_metadata/swt_product_line.txt;

create or replace file format zmd_file_format_1
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE;

select $1
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|' 
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE;

select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

create or replace file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'
TRIM_SPACE = TRUE; 
    
CREATE VIEW  ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_COORDINATION AS(    
    select 
     REPLACE($1,chr(13)||chr(10)) AS PRODUCT_CODE
    ,REPLACE($2,chr(13)||chr(10)) AS HAS_MATCHING_SWEATSUIT
    from @product_metadata/product_coordination_suggestions.txt
    (file_format => zmd_file_format_3)
    WHERE PRODUCT_CODE <> '');

create view zenas_athleisure_db.products.sweatsuit_sizes as (
select REPLACE($1,chr(13)||chr(10)) as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '');

create view zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE as ( 
    select 
    REPLACE($1,chr(13)||chr(10)) AS PRODUCT_CODE
    ,REPLACE($2,chr(13)||chr(10)) AS HANDBAND_DESCRIPTION
    ,$3 AS WAISTBAND_DESCRIPTION 
    from @product_metadata/swt_product_line.txt
    (file_format => zmd_file_format_2)
    WHERE PRODUCT_CODE <> ''
);

select * from sweatsuit_sizes;

select product_code, has_matching_sweatsuit
from zenas_athleisure_db.products.sweatband_coordination;

select sizes_available
from zenas_athleisure_db.products.sweatsuit_sizes;