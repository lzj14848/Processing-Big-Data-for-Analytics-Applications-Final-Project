

javac -classpath `yarn classpath` -d . CountRecsMapper.java
 
javac -classpath `yarn classpath` -d . CountRecsReducer.java
 
javac -classpath `yarn classpath`:. -d . CountRecs.java
 
jar -cvf CountRecs.jar *.class

hdfs dfs -rm -r -f hw7/outputPart1_1

hadoop jar CountRecs.jar CountRecs hw7/outputPart2/part-r-00000 /user/netID/hw7/outputPart1_1
