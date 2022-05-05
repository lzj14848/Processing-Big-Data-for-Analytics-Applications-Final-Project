rm -rf *.class *.jar

javac -classpath `yarn classpath` -d . CountRecsMapper.java
 
javac -classpath `yarn classpath` -d . CountRecsReducer.java
 
javac -classpath `yarn classpath`:. -d . CountRecs.java
 
jar -cvf CountRecs.jar *.class

hdfs dfs -rm -r -f hw8/output/count_cleaned

hadoop jar CountRecs.jar CountRecs hw8/input/cleaned_homeless.csv /user/netID/hw8/output/count_cleaned

hdfs dfs -cat /user/netID/hw8/output/count_cleaned/part-r-00000
