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
df2.show()
df2.summary().show()

# https://stackoverflow.com/a/44413456
df2.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df2.columns)).show()

# df2.write.csv("join_dataset")
df2.toPandas().to_csv("homeless_joined_ready.csv",index=False)

