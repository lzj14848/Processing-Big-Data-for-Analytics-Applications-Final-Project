# Data profiling after cleaning the raw data

rm -rf *.class *.jar

javac -classpath `yarn classpath` -d . CountRecsMapper.java
 
javac -classpath `yarn classpath` -d . CountRecsReducer.java
 
javac -classpath `yarn classpath`:. -d . CountRecs.java

jar -cvf CountRecs.jar *.class

hdfs dfs -rm -r -f hw7/output_on_cleaned

hadoop jar CountRecs.jar CountRecs hw7/input/cleaned_summon.csv /user/netID/hw7/output_on_cleaned

# hdfs dfs -rm /user/netID/hw8/cleaned_summon.csv
# hdfs dfs -mv /user/netID/hw7/input/cleaned_summon.csv /user/netID/hw8