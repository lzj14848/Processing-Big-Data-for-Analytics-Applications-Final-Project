import matplotlib.pyplot as plt
# import pandas as pd
from pyspark.sql.functions import *

# We use spark.read.csv() to read csv file in spark
df = spark.read.csv("/user/netID/cleaned_temperature.csv")


df = df.withColumnRenamed("_c0", "log_date")
df = df.withColumnRenamed("_c1", "DailyAverageDewPointTemperature")
df = df.withColumnRenamed("_c2", "DailyAverageDryBulbTemperature")
df = df.withColumnRenamed("_c3", "DailyAverageWetBulbTemperature")
df = df.withColumnRenamed("_c4", "DailyCoolingDegreeDays")
df = df.withColumnRenamed("_c5", "DailyDepartureFromNormalAverageTemperature")
df = df.withColumnRenamed("_c6", "DailyHeatingDegreeDays")
df = df.withColumnRenamed("_c7", "DailyMaximumDryBulbTemperature")
df = df.withColumnRenamed("_c8", "DailyMinimumDryBulbTemperature")


df = df.withColumn("log_date", to_date(col('log_date'), 'yyyy-MM-dd'))


df = df.withColumn("DailyAverageDewPointTemperature",col("DailyAverageDewPointTemperature").cast("float"))

df = df.withColumn("DailyAverageDryBulbTemperature",col("DailyAverageDryBulbTemperature").cast("float"))

df = df.withColumn("DailyAverageWetBulbTemperature",col("DailyAverageWetBulbTemperature").cast("float"))

df = df.withColumn("DailyCoolingDegreeDays",col("DailyCoolingDegreeDays").cast("float"))

df = df.withColumn("DailyDepartureFromNormalAverageTemperature",col("DailyDepartureFromNormalAverageTemperature").cast("float"))

df = df.withColumn("DailyHeatingDegreeDays",col("DailyHeatingDegreeDays").cast("float"))

df = df.withColumn("DailyMaximumDryBulbTemperature",col("DailyMaximumDryBulbTemperature").cast("float"))

df = df.withColumn("DailyMinimumDryBulbTemperature",col("DailyMinimumDryBulbTemperature").cast("float"))

df.show()

# https://stackoverflow.com/a/44413456
df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns)).show()


df = df.drop("DailyAverageDewPointTemperature")
df = df.drop("DailyAverageWetBulbTemperature")
df = df.drop("DailyCoolingDegreeDays")
df = df.drop("DailyDepartureFromNormalAverageTemperature")
df = df.drop("DailyHeatingDegreeDays")
df = df.drop("DailyMaximumDryBulbTemperature")
df = df.drop("DailyMinimumDryBulbTemperature")

df.show()
df.summary().show()

# https://stackoverflow.com/a/44413456
df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns)).show()



df.toPandas().to_csv("temperature_joined_ready.csv",index=False)