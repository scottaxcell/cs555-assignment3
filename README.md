# CS 555: Distributed Systems -- Assignment 3

Analyzing On-Time Performance Of Commercial Flights In The United States Using MapReduce

# HDFS

First time configuration: `$HADOOP_HOME/bin/hdfs namenode -format`

Trust, but verify: `$HADOOP_HOME/bin/hdfs dfsadmin -report`

Web portal: http://frankfort:53501/dfshealth.html#tab-overview

# Yarn

Web portal: http://frankfort:53507/cluster


# MapReduce Client

```
export HADOOP_CONF_DIR=${HOME}/cs555-assignment3/client-config
```

# Build and Run
```
gradle build && \
$HADOOP_HOME/bin/hdfs dfs -rm -r /home/question*; \
$HADOOP_HOME/bin/hadoop jar build/libs/cs555-assignment3.jar cs555.hadoop.JobRunner

rm -f q*_results.txt; \
$HADOOP_HOME/bin/hdfs dfs -get /home/question1_tod/part-r-00000 q1_tod_results.txt && \
$HADOOP_HOME/bin/hdfs dfs -get /home/question1_dow/part-r-00000 q1_dow_results.txt && \
$HADOOP_HOME/bin/hdfs dfs -get /home/question1_toy/part-r-00000 q1_toy_results.txt && \
$HADOOP_HOME/bin/hdfs dfs -get /home/question3/part-r-00000 q3_results.txt && \
$HADOOP_HOME/bin/hdfs dfs -get /home/question4/part-r-00000 q4_results.txt && \
$HADOOP_HOME/bin/hdfs dfs -get /home/question5/part-r-00000 q5_results.txt && \
$HADOOP_HOME/bin/hdfs dfs -get /home/question6/part-r-00000 q6_results.txt && \
$HADOOP_HOME/bin/hdfs dfs -get /home/question7/part-r-00000 q7_results.txt
```