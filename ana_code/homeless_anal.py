# We use spark.read.csv() to read csv file in spark
df = spark.read.csv("/user/netID/hw8/input/cleaned_homeless.csv")

df = df.toDF(*["Created_Date", "Closed_Date",
              "Incident_Addr", "City",
              "Resolution_Date", "Borough"])



from pyspark.sql.functions import *
df = df.withColumn("Created_Date", to_date(col("Created_Date"),"MM/dd/yyyy"))
df = df.withColumn("Borough", regexp_replace('Borough', r'\t', ''))
new_df = df.groupBy("Created_Date", "Borough").count()
new_df = new_df.withColumn("Year", year(new_df.Created_Date))

df2 = new_df.filter((new_df.Year  == 2018) | (new_df.Year  == 2019))
df2 = df2.withColumn("Month", month(df2.Created_Date))

# Analysis 1:
# total number of report of homeless encampments for each borough during 2018-2019:
borough_anal = df2.groupBy("Borough").count()
borough_anal.orderBy(col("count").desc()).show()

# Analysis 2:
# variation of number of reports through months in 2018:
df_2018 = df2.filter(df2.Year == 2018)
df_2018 = df_2018.groupBy("Month").sum().orderBy("Month")
df_2018 = df_2018.withColumnRenamed("sum(count)", "2018_month_count")
df_2018.select("Month", "2018_month_count").show()
# variation of number of reports through months in 2019:
df_2019 = df2.filter(df2.Year == 2019)
df_2019 = df_2019.groupBy("Month").sum().orderBy("Month")
df_2019 = df_2019.withColumnRenamed("sum(count)", "2019_month_count")
df_2019.select("Month", "2019_month_count").show()

# Analysis 3:
# comparison of total number of reports of homeless encampments between year 2018 and year 2019
df_year = df2.groupBy("Year").sum()
df_year = df_year.withColumnRenamed("sum(count)", "total_count")
df_year.select("Year", "total_count").show()

