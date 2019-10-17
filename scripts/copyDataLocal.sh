$HADOOP_HOME/bin/hdfs dfs -mkdir /home/data
$HADOOP_HOME/bin/hdfs dfs -mkdir /home/data/main
$HADOOP_HOME/bin/hdfs dfs -mkdir /home/data/supplementary

$HADOOP_HOME/bin/hdfs dfs -cp /data/main/1988.csv /home/data/main
$HADOOP_HOME/bin/hdfs dfs -cp /data/supplementary/airports.csv /home/data/supplementary
$HADOOP_HOME/bin/hdfs dfs -cp /data/supplementary/carriers.csv /home/data/supplementary