# Temperature dataset individual analysis
from pyspark.sql.functions import *
df = spark.read.option("header", "true").csv("/user/netID/temperature_joined_ready.csv")


df = df.withColumn("log_date", to_date(col('log_date'), 'yyyy-MM-dd'))

df = df.withColumn("DailyAverageDryBulbTemperature",col("DailyAverageDryBulbTemperature").cast("float"))

df = df.withColumn('year',year(df.log_date))
df = df.withColumn('month',month(df.log_date))


# Analysis 1: Retrieve the dates with the lowest temperature in 2018
df.filter(df.year == 2018).sort("DailyAverageDryBulbTemperature").show()

# Analysis 2: Retrieve the dates with the highest temperature in 2018
df.filter(df.year == 2018).orderBy("DailyAverageDryBulbTemperature", ascending=False).show()

# Analysis 3: Retrieve the dates with the lowest temperature in 2019
df.filter(df.year == 2019).sort("DailyAverageDryBulbTemperature").show()

# Analysis 4: Retrieve the dates with the highest temperature in 2019
df.filter(df.year == 2019).orderBy("DailyAverageDryBulbTemperature", ascending=False).show()

# Analysis 5: Retrieve the average temperature in 2018
df.filter(df.year == 2018).agg(avg(col("DailyAverageDryBulbTemperature"))).show()

# Analysis 6: Retrieve the average temperature in 2019
df.filter(df.year == 2019).agg(avg(col("DailyAverageDryBulbTemperature"))).show()

# Analysis 7: Retrieve the average temperature in all months in 2018 and 2019
df.groupBy(df.year, df.month).mean("DailyAverageDryBulbTemperature").sort(df.year, df.month).show(24)

# Analysis 8: Retrieve the max temperature in all months in 2018 and 2019
df.groupBy(df.year, df.month).max("DailyAverageDryBulbTemperature").sort(df.year, df.month).show(24)

# Analysis 9: Retrieve the min temperature in all months in 2018 and 2019
df.groupBy(df.year, df.month).min("DailyAverageDryBulbTemperature").sort(df.year, df.month).show(24)