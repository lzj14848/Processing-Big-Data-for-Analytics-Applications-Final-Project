

javac -classpath `yarn classpath` -d . CleanMapper.java
 
javac -classpath `yarn classpath` -d . CleanReducer.java
 
javac -classpath `yarn classpath`:. -d . Clean.java
 
jar -cvf Clean.jar *.class

hdfs dfs -rm -r -f hw7/outputPart2

hadoop jar Clean.jar Clean hw7/input /user/netID/hw7/outputPart2

