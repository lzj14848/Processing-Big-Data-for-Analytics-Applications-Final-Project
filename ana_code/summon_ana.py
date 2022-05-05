from pyspark.sql.functions import *

# import pandas as pd
df = spark.read.csv("/user/netID/hw8/cleaned_summon.csv")
# df = spark.read.csv("/Users/katelynwang/cs476file/final_project/hw8/cleaned_input.csv")

df = df.toDF(*["key","date","offense","ageGroup","sex","race","borough","longitude_latitude"])
df = df.filter(df.borough!='null')

# DATA PROFILING & CLEANING
df.summary().show()

# Number of null entries in the variables that we care about
df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns)).show()

# Drop Nan
df = df.dropna()

df = df.withColumn('date',to_date(col("date"),"MM/dd/yyyy"))
df = df.withColumn('year',date_format('date','yyyy'))
df = df.withColumn('month',date_format('date','M'))

df = df.filter((df.year=='2018')|(df.year=='2019'))
df = df.filter(col("borough").isNotNull())
df = df.replace('NEW YORK','MANHATTAN')
df = df.drop('longitude_latitude')

# SOME SINGLE DATSET ANALYSIS
df.groupBy("borough").count().show() # MAHATTAN has the most summons record, BROOKLYN the second
df.groupBy("borough","date").count().show()
df.groupBy("year","month").count().show()

# MOST COMMON OFFENSE
df.groupBy("offense").count().sort(col('count').desc()).show() # ALCOHOLIC BEVERAGE IN PUBLIC

# MOST COMMON OFFENZSE BY GENDER
df.filter(df.sex == 'M').groupBy("offense").count().sort(col('count').desc()).show()
df.filter(df.sex == 'F').groupBy("offense").count().sort(col('count').desc()).show()

# WAHT AGEGROUP HAS THE HIGHEST NUMBER OF SUMMON, IN WHICH AREA?
df.groupBy("ageGroup","borough").count().sort(col('count').desc()).show() # BRONX, 25-44 yrs
df.groupBy("ageGroup","offense").count().sort(col('count').desc()).show()

# RACE ANALYSIS
df.groupBy("race","borough").count().sort(col('count').desc()).show()
df.groupBy("race","offense").count().sort(col('count').desc()).show()
df.groupBy("race","ageGroup","borough").count().sort(col('count').desc()).show()


