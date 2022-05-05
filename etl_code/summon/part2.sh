# Cleaning the data

rm -rf *.class *.jar

javac -classpath `yarn classpath` -d . CleanMapper.java
 
javac -classpath `yarn classpath` -d . CleanReducer.java
 
javac -classpath `yarn classpath`:. -d . Clean.java

jar -cvf Clean.jar *.class

hdfs dfs -rm -r -f hw7/output1

hadoop jar Clean.jar Clean hw7/input /user/netID/hw7/output1

# hdfs dfs -mv hw7/output1/part-r-00000 hw7/input
# hdfs dfs -mv hw7/input/part-r-00000 hw7/input/cleaned_summon.csv # change the name and suffix of the cleaned data file
