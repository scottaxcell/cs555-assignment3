# CS 555: Distributed Systems -- Assignment 3

Analyzing On-Time Performance Of Commercial Flights In The United States Using MapReduce

# HDFS

First time configuration: `$HADOOP_HOME/bin/hdfs namenode -format`

Trust, but verify: `$HADOOP_HOME/bin/hdfs dfsadmin -report`

Web portal: http://tokyo:53501/dfshealth.html#tab-overview

# Yarn

Web portal: http://tokyo:53507/cluster

# Word Count Example

```
$HADOOP_HOME/bin/hadoop jar build/libs/cs455-wordcount-sp19.jar cs455.hadoop.wordcount.WordCountJob /cs455/books /cs455/wordcount-out
$HADOOP_HOME/bin/hdfs dfs -ls /cs455/wordcount-out
$HADOOP_HOME/bin/hdfs dfs -cat /cs455/wordcount-out/part-r-00000 > part-r-00000
```

# MapReduce Client

```
export HADOOP_CONF_DIR=${HOME}/cs555-assignment3/client-config
```

# Build
```
gradle build
```

# Run
```
$HADOOP_HOME/bin/hdfs dfs -rm -r /home/question1 && $HADOOP_HOME/bin/hadoop jar build/libs/cs555-assignment3.jar cs555.hadoop.JobRunner
```