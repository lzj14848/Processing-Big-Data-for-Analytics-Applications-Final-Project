# Processing-Big-Data-for-Analytics-Applications-Final-Project
# Please notice that some information was removed or replaced due to privacy issues

# Relationship Between Temperature, Homeless Encampments, and Criminal Activities in New York City (2018 - 2019)

## Data Source 1: [U.S. Local Climatological Data: NY CITY CENTRAL PARK](https://www.ncei.noaa.gov/access/search/data-search/local-climatological-data?bbox=40.965,-74.257,40.465,-73.757&startDate=2018-01-01T00:00:00&endDate=2019-12-31T23:59:59&dataTypes=DailyAverageDewPointTemperature&dataTypes=DailyAverageDryBulbTemperature&dataTypes=DailyAverageWetBulbTemperature)

**Input Location: `/user/temperature/hw7/input/temp2018.csv` AND `/user/temperature/hw7/input/temp2019.csv` in HDFS**

1. In `profiling_code/temperature`, for initial profiling:
    - run `chmod +x initial_profiling.sh`
    - run `./initial_profiling.sh`
    - run `hdfs dfs -cat /user/temperature/hw7/outputPart1_1/part-r-00000` to check the result
2. In `etl_code/temperature`, for initial cleaning:
    - run `chmod +x part2.sh`
    - run `./part2.sh`
3. In `profiling_code/temperature`, for profiling of the initial cleaning:
    - run `chmod +x part1.sh`
    - run `./part1.sh`
    - run `hdfs dfs -cat /user/temperature/hw7/outputPart1_1/part-r-00000` to check the result
4. Turn the result into a csv file:
    - run `hdfs dfs -mv /user/temperature/hw7/outputPart2/part-r-00000 /user/temperature/cleaned_temperature.csv`
5. In `profiling_code/temperature`, for further cleaning and profiling in pySpark:
    - run `module load python/gcc/3.7.9`
    - run `PYTHONSTARTUP=temperature_profile_clean.py pyspark --deploy-mode client`
    - run `exit()`
6. Upload the result csv file `temperature_joined_ready.csv` to HDFS:
    - run `hdfs dfs -put temperature_joined_ready.csv /user/temperature`
7. In `ana_code`, for analysis the temperature dataset:
    - run `module load python/gcc/3.7.9`
    - run `PYTHONSTARTUP=temperature_ana.py pyspark --deploy-mode client`

## Data Source 2: [NYPD Criminal Court Summons (Historic)](https://data.cityofnewyork.us/Public-Safety/NYPD-Criminal-Court-Summons-Historic-/sv2w-rv3k)

### Initial Cleaning and Profiling: MapReduce

**Input Location on hdfs**: `/user/summon/hw7/input/NYPD_Criminal_Court_Summons__Historic_.csv`

1. In `profiling_code/summon`, for initial profiling:
    - run `chmod +x initial_profiling.sh`
    - run `./initial_profiling.sh`
    - run `hdfs dfs -cat /user/summon/hw7/output/part-r-00000` to check the result
2. In `etl_code/summon`, for initial cleaning:
    - run `chmod +x part2.sh`
    - run `./part2.sh`
    - the result is in `/user/summon/hw7/output1/part-r-00000`
    - moved the resulting data in step 2 to `/user/summon/hw7/input` as input for post-cleaning profiling:
        - run `hdfs dfs -mv hw7/output1/part-r-00000 hw7/input`
    - Renamed the data file: `hdfs dfs -mv hw7/input/part-r-00000 hw7/input/cleaned_summon.csv`
3. In `profiling_code/summon`, for profiling of cleaned data:
    - run `chmod +x part1.sh`
    - run `./part1.sh`
    - run `hdfs dfs -cat /user/summon/hw7/output_on_cleaned/part-r-00000`
4. Move the resulting csv file (moved and renamed in step 2) to home directory on hdfs for next step 
    - run `hdfs dfs -rm /user/summon/hw8/cleaned_summon.csv`
    - run `hdfs dfs -mv /user/summon/hw7/input/cleaned_summon.csv /user/summon/hw8`

### Further Cleaning, Profiling, and Single Dataset Analysis: PySpark

1. In `profiling_code/summon`, for further cleaning and profiling in pySpark:
    - run `module load python/gcc/3.7.9`
    - run `PYTHONSTARTUP=summon_profile_clean.py pyspark --deploy-mode client`
2. Exit spark, then upload the result csv file `summon_joined_ready.csv` to HDFS:
    - run `hdfs dfs -put summon_joined_ready.csv /user/summon`

**[ Using summon_jointed_read.csv, obtain `merge_table.csv`  from the join table step]** 

1. In `ana_code`, for analysis the summon dataset alone:
    - `module load python/gcc/3.7.9`
    - `PYTHONSTARTUP=summon_ana.py pyspark --deploy-mode client`

## Data Source 3: [NYC OpenData: Homeless Encampments](https://data.cityofnewyork.us/Social-Services/Homeless-Encampments/jufg-yyky)

### Initial Cleaning and Profiling: MapReduce

Input Location: `/user/homeless/hw8/input/Homeless_Encampments.csv` in HDFS

1. In `profiling_code/homeless`, for initial profiling:
    - run `chmod +x initial_profiling.sh`
    - run `./initial_profiling.sh`
2. In `etl_code/homeless`, for initial cleaning:
    - run `chmod +x clean_dataset.sh`
    - run `./clean_dataset.sh`
3. In `profiling_code/homeless`, for profiling of initial cleaning:
    - run  `hdfs dfs -mv /user/homeless/hw8/output/clean/part-r-00000 /user/homeless/hw8/input/cleaned_homeless.csv`
    - run `chmod +x sec_profiling.sh`
    - run `./sec_profiling.sh`

### Further Cleaning, Profiling, and Single Dataset Analysis: PySpark

1. In `profiling_code/homeless`, for further cleaning and profiling in pySpark:
    - run `module load python/gcc/3.7.9`
    - run `PYTHONSTARTUP=homeless_profile_clean.py pyspark --deploy-mode client`
2. Upload the result csv file `homeless_joined_ready.csv` to HDFS:
    - run `hdfs dfs -put homeless_joined_ready.csv /user/homeless`
3. in `ana_code`, for analysis the homeless dataset alone:
    - run `module load python/gcc/3.7.9`
    - run `PYTHONSTARTUP=homeless_anal.py pyspark --deploy-mode client`

# Merge Datasets and Further Analysis

### **This is part is done after the individual merge-ready datasets are uploaded to the HDFS of user temperature.**

### **The location is /user/temperature/three_datasets**

## Join the 3 tables in Hive

1. In `data_ingest`:
    - run `beeline`
    - run `!connect jdbc:hive2://hm-1.hpc.nyu.edu:10000/` and input username and password
    - run `use <netid>`
    - run the commands in `hive_command.sql` to merge tables
2. In Peel, to export the hive table as a csv file:
    - run `beeline -u jdbc:hive2://hm-1.hpc.nyu.edu:10000/ -n temperature --outputformat=csv2 --showHeader=false -e 'use temperature; select * from merged' | sed 's/[\\t]/,/g' > merged_table.csv`
3. Upload the result csv file `merged_table.csv` to HDFS:
    - run `hdfs dfs -put merged_table.csv /user/temperature`
4. In `ana_code`, for analysis of the merged datasets:
    - run `module load python/gcc/3.7.9`
    - run `PYTHONSTARTUP=merged_ana.py pyspark --deploy-mode client`
