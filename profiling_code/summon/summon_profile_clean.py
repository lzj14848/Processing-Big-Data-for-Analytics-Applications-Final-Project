import matplotlib.pyplot as plt
from pyspark.sql.functions import *

# import pandas as pd
df = spark.read.csv("/user/netID/hw8/cleaned_summon.csv")

df = df.toDF(*["key","date","offense","ageGroup","sex","race","borough","longitude_latitude"])
df = df.filter(df.borough!='null')

# DATA PROFILING & CLEANING
df.summary().show()

# Number of null entries in the variables that we care about
df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns)).show()

# Some more data cleaning
from pyspark.sql.functions import *

# Date type extraction, creating three more columns: date, year, and month
dates = df.select(col("date"),to_date(col("date"),"MM/dd/yyyy")).alias("date").show()
df = df.withColumn('date',to_date(col("date"),"MM/dd/yyyy"))
df = df.withColumn('year',date_format('date','yyyy'))
df = df.withColumn('month',date_format('date','M'))

# Further filter the data to only the summons in 2018 and 2019
df = df.filter((df.year=='2018')|(df.year=='2019'))
df = df.filter(col("borough").isNotNull())
df = df.replace('NEW YORK','MANHATTAN')
df = df.drop('longitude_latitude')
df.groupBy("borough").count().show()

#UNQIUE VALUES
df.select('race').distinct().show()
df.select('ageGroup').distinct().show()
df.select('sex').distinct().show()
df.select('offense').distinct().show()
df.select('offense').distinct().count() # 569 unique summons


# Prepare table for exporting joined_ready data to put into HiveSQL
exp_df = df.groupBy("borough","date").count()
exp_df.show()
exp_df.toPandas().to_csv("summon_joined_ready.csv",index=False)


