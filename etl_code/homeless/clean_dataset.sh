rm -rf *.class *.jar

javac -classpath `yarn classpath` -d . CleanMapper.java

javac -classpath `yarn classpath` -d . CleanReducer.java

javac -classpath `yarn classpath`:. -d . Clean.java

jar -cvf clean.jar *.class

hdfs dfs -rm -r -f hw8/output/clean

hadoop jar clean.jar Clean hw8/input/Homeless_Encampments.csv /user/netID/hw8/output/clean

hdfs dfs -cat /user/netID/hw8/output/clean/part-r-00000
