# Data profiling before cleaning the raw data

rm -rf *.class *.jar

javac -classpath `yarn classpath` -d . CountRecsMapper.java
 
javac -classpath `yarn classpath` -d . CountRecsReducer.java
 
javac -classpath `yarn classpath`:. -d . CountRecs.java

jar -cvf CountRecs.jar *.class

hdfs dfs -rm -r -f hw7/output

hadoop jar CountRecs.jar CountRecs hw7/input/NYPD_Criminal_Court_Summons__Historic_.csv /user/netID/hw7/output