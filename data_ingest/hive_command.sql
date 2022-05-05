

DROP TABLE IF EXISTS temperature;
DROP TABLE IF EXISTS homeless;
DROP TABLE IF EXISTS summon;


CREATE EXTERNAL TABLE temperature(log_date DATE, DailyAverageDryBulbTemperature FLOAT) COMMENT 'Temperature' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/zl2527/three_datasets/temperature';

CREATE EXTERNAL TABLE homeless(log_date DATE, borough STRING, encampment_count INT, year INT) COMMENT 'Homeless Encampments' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/zl2527/three_datasets/homeless';

CREATE EXTERNAL TABLE summon(borough STRING, log_date DATE, summon_count INT) COMMENT 'Summons' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/zl2527/three_datasets/summon';



CREATE TABLE merged COMMENT 'Merged table' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE AS SELECT summon.log_date, summon.borough, summon.summon_count, homeless.encampment_count, temperature.dailyaveragedrybulbtemperature FROM summon FULL OUTER JOIN homeless ON summon.log_date=homeless.log_date AND summon.borough=homeless.borough JOIN temperature ON summon.log_date=temperature.log_date;






