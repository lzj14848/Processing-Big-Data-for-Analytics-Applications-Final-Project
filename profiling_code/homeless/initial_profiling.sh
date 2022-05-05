rm -rf *.class *.jar

javac -classpath `yarn classpath` -d . CountRecsMapper.java
 
javac -classpath `yarn classpath` -d . CountRecsReducer.java
 
javac -classpath `yarn classpath`:. -d . CountRecs.java
 
jar -cvf CountRecs.jar *.class

hdfs dfs -rm -r -f hw8/output/count_raw

hadoop jar CountRecs.jar CountRecs hw8/input/Homeless_Encampments.csv /user/netID/hw8/output/count_raw

hdfs dfs -cat /user/netID/hw8/output/count_raw/part-r-00000
