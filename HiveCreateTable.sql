SET hive.mapred.supports.subdirectories = TRUE;

SET mapred.input.dir.recursive=TRUE;

create database financial;
use financial;

CREATE EXTERNAL TABLE Financial_Data(
Symbol String,
Name String,
Sector String,
Price Double,
PricePerEarnings Double,
DividendYield Double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://emr-financial-bucket/input/'
TBLPROPERTIES ("skip.header.line.count" = "1");

---------------------AGGREGATION TABLE -------------------------
use financial;
CREATE TABLE Financial_Data_Yield_Agg
STORED AS PARQUET
LOCATION 's3://emr-financial-bucket/aggregation/'
as
SELECT a.*
FROM Financial_Data as a
INNER JOIN 
(SELECT sector, MAX(dividendyield) AS maxdividendyield FROM Financial_Data 
GROUP BY sector) as SectorYields
ON a.sector = SectorYields.sector
AND a.dividendyield = SectorYields.maxdividendyield