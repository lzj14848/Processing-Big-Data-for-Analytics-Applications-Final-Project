# homeless and summons datasets merged analysis
from pyspark.sql.functions import *
df = spark.read.csv("/user/netID/merged_table.csv")

df = df.withColumnRenamed("_c0", "log_date")
df = df.withColumnRenamed("_c1", "borough")
df = df.withColumnRenamed("_c2", "summon_count")
df = df.withColumnRenamed("_c3", "encampment_count")
df = df.withColumnRenamed("_c4", "temperature")

df = df.withColumn("log_date", to_date(col('log_date'), 'yyyy-MM-dd'))
df = df.withColumn("summon_count",col("summon_count").cast("int"))
df = df.withColumn("encampment_count",col("encampment_count").cast("int"))
df = df.withColumn("temperature",col("temperature").cast("float"))

df = df.withColumn('year',year(df.log_date))
df = df.withColumn('month',month(df.log_date))

# https://stackoverflow.com/a/44413456
df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns)).show()

# fill the encampment_count null value with 0, as null means 0 count.
df = df.na.fill(value=0)

# Analysis 1: Correlation Index Between summon_count and encampment_count in 2018
df.filter(df.year == 2018).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

# Analysis 2: Correlation Index Between summon_count and encampment_count in 2019
df.filter(df.year == 2019).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

# Analysis 3: Correlation Index Between summon_count and encampment_count in Bronx in 2018 and 2019
df.filter(df.borough == "BRONX").corr("summon_count", "encampment_count")

# Analysis 4: Correlation Index Between summon_count and encampment_count in Brooklyn in 2018 and 2019
df.filter(df.borough == "BROOKLYN").corr("summon_count", "encampment_count")

# Analysis 5: Correlation Index Between summon_count and encampment_count in Manhattan in 2018 and 2019
df.filter(df.borough == "MANHATTAN").corr("summon_count", "encampment_count")

# Analysis 6: Correlation Index Between summon_count and encampment_count in Queens in 2018 and 2019
df.filter(df.borough == "QUEENS").corr("summon_count", "encampment_count")

# Analysis 7: Correlation Index Between summon_count and encampment_count in Staten Island in 2018 and 2019
df.filter(df.borough == "STATEN ISLAND").corr("summon_count", "encampment_count")

# Analysis 8: Correlation Index Between summon_count and encampment_count in different months
df.filter(df.month == 1).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 2).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 3).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 4).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 5).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 6).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 7).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 8).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 9).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 10).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 11).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")

df.filter(df.month == 12).groupBy("log_date").sum("summon_count","encampment_count").sort(df.log_date).corr("sum(summon_count)", "sum(encampment_count)")




# Analysis 1: Homeless/Encampments count by month in 2018:
homeless_count = df.filter(df.year == 2018).groupBy("month").sum("encampment_count").sort(df.month)
homeless_count = homeless_count.withColumnRenamed("sum(encampment_count)", "encampment_count")
homeless_count.show()

# Analysis 2: Average Temperature by month in 2018:
average_temp = df.filter(df.year == 2018).groupBy("month").mean("temperature").sort(df.month)
average_temp = average_temp.withColumnRenamed("avg(temperature)", "average_temperature")
average_temp.show()

# Analysis 3: Correlation Index Between temperature and encampment_count across months in 2018
tmp_encamp = homeless_count.join(average_temp, on=["month"])
tmp_encamp.show()
tmp_encamp.corr("encampment_count", "average_temperature")

# Analysis 4: Homeless/Encampments count by month in 2019:
homeless_count2 = df.filter(df.year == 2019).groupBy("month").sum("encampment_count").sort(df.month)
homeless_count2 = homeless_count2.withColumnRenamed("sum(encampment_count)", "encampment_count")

# Analysis 5: Average Temperatire by month in 2019:
average_temp2 = df.filter(df.year == 2019).groupBy("month").mean("temperature").sort(df.month)
average_temp2 = average_temp2.withColumnRenamed("avg(temperature)", "average_temperature")

# Analysis 6: Correlation Index Between temperature and encampment_count across months in 2019
tmp_encamp2 = homeless_count2.join(average_temp2, on=["month"])
tmp_encamp2.show()
tmp_encamp2.corr("encampment_count", "average_temperature")

# Analysis 7: Seasonal Homeless/Encampments count:
df.filter(df.temperature < 32).groupby('borough').sum('encampment_count').show()
df.filter((df.temperature > 32)&(df.temperature < 50)).groupby('borough').sum('encampment_count').show()
df.filter((df.temperature > 50)&(df.temperature < 70)).groupby('borough').sum('encampment_count').show()
df.filter((df.temperature > 70)&(df.temperature < 85)).groupby('borough').sum('encampment_count').show()
df.filter((df.temperature > 85 )).groupby('borough').sum('encampment_count').show()





# Remove the encampment column
df_sw = df.drop('encampment_count')
df_sw.show()

# Summon Count by month
df_sw.filter(df.year == 2018).groupby('month','year').count().sort('count').show()
df_sw.filter(df.year == 2019).groupby('month','year').count().sort('count').show()

# BROOKLYN's ANALYSIS
df.filter(df.borough == 'BROOKLYN').groupby('month').sum('summon_count').sort(df.month).show()
df.filter(df.borough == 'BROOKLYN').groupby('month').avg('temperature').sort(df.month).show()

# MANHATTAN's ANALYSIS
df.filter(df.borough == 'MANHATTAN').groupby('month').sum('summon_count').sort(df.month).show()
df.filter(df.borough == 'MANHATTAN').groupby('month').avg('temperature').sort(df.month).show()

# Summon count by borough and month, with average temperature of the month
df0 = df_sw.groupby('borough','month').sum('summon_count')
df1 = df_sw.groupby('borough','month').avg('temperature')
g1 = df0.join(df1,(df1.borough == df0.borough) & (df1.month == df0.month),how='left').drop(df0.month).drop(df0.borough)
g1.sort(col('sum(summon_count)').desc()).show()

# Summon count by Month and Year
df2 = df.groupby('month','year').sum('summon_count')
df3 = df.groupby('month','year').avg('temperature')
g2 = df2.join(df3,(df3.month == df2.month)&(df3.year == df2.year),how='inner').drop(df3.month).drop(df3.year).sort(col('month').asc())
g2.show()

# Correlation between summon count and average temperature when grouping by month and year
g2.corr("sum(summon_count)", "avg(temperature)") # 0.24477209065308664, low
g2.corr("sum(summon_count)", "month") # -0.6772906248080454

# Summon count only by month
df4 = df.groupby('month').sum('summon_count')
df5 = df.groupby('month').avg('temperature')
g3 = df4.join(df5,(df5.month == df4.month),how='inner').drop(df4.month).sort(col('month').asc())
g3.show()

# correlation between summon count and average temperature when grouping by month and year
g3.corr("sum(summon_count)", "avg(temperature)") # 0.26555239730602886
g3.corr("sum(summon_count)", "month") # -0.7374154530496048

# Seasonal summon count (classified by daily average degree)
df.filter(df.temperature < 32).groupby('borough').sum('summon_count').show()
df.filter((df.temperature > 32)&(df.temperature < 50)).groupby('borough').sum('summon_count').show()
df.filter((df.temperature > 50)&(df.temperature < 70)).groupby('borough').sum('summon_count').show()
df.filter((df.temperature > 70)&(df.temperature < 85)).groupby('borough').sum('summon_count').show()
df.filter((df.temperature > 85 )).groupby('borough').sum('summon_count').show()

df.corr("summon_count", "temperature") # 0.03258396014385423

# Summary GrSummon Count by Borough (for visualization)
s0 = df.groupby('borough').sum('summon_count')
s0.show()
summarygraph1 = s0.toDF('borough','date').toPandas()



# MERGE DATA VISUALIZATION (SUMMON vs. TEMPERATURE)

# import seaborn as sns

# # Summary Summon Count by Borough
# fig = plt.figure()
# ax = fig.add_axes([0,0,1,1])
# borough = summarygraph1['borough']
# students = summarygraph1['date']
# ax.bar(borough,students)
# plt.show()

# # Summon Count Bar Plot
# gg2 = g2.toPandas()
# labels = np.linspace(1, 12, num=12)
# data18 = gg2[gg2['year']==2018]['sum(summon_count)']
# data19 = gg2[gg2['year']==2019]['sum(summon_count)']
# width = 0.5
# fig, ax = plt.subplots()
# ax.bar(labels, data18, width, label='2018')
# ax.bar(labels, data19, width, bottom=data18, label='2019')
# ax.set_ylabel('Scores')
# ax.set_title('Summons Count by month')
# ax.legend()
# plt.show()

# # Summon Count vs. Tempearture Scatterplot (Each point already grouped by month & year)
# gg3 = df.toPandas()
# x = gg2['avg(temperature)']
# y = gg2['sum(summon_count)'] 
# plt.scatter(x, y)
# plt.show()

# # Summon Count vs. Tempearture Scatterplot Colored by years, stacked (Each point representing a day)
# all_count = gg3['summon_count']
# all_tmp = gg3['temperature']
 
# sns.set(style='whitegrid')
# sns.set(rc = {'figure.figsize':(15,8)})
# sns.scatterplot(x="summon_count",
#                     y="temperature",
#                     hue="year",
#                     palette = "colorblind",
#                     data=gg3)

# # Summon Count vs. Tempearture Scatterplot Colored by boroughs, stacked (Each point representing a day)
# sns.scatterplot(x="summon_count",
#                     y="temperature",
#                     hue="borough",
#                     palette = "colorblind",
#                     data=gg3)

# # Grouped Scatter Plot 
# with sns.axes_style("white"):
#     scatter = sns.FacetGrid(gg3, row="year", col="borough", margin_titles=True, height=4)
# scatter.map(sns.scatterplot, "temperature","summon_count", color="#334488")
# scatter.set_axis_labels("Average temperature of the day","Summons Count")
# sns.set(rc = {'figure.figsize':(15,8)})
# scatter.set(xticks=np.linspace(0,100,9), yticks=[0,50,100,150,200,250])
# # scatter.figure.subplots_adjust(wspace=.02, hspace=.02)

