# Hadoop MapReduce on Private Cloud Environment


The objective of this project is to implement a Hadoop MapReduce environment on an isolated private cloud or a private cloud environment. 

> `Hadoop MapReduce` is a software framework 
 for easily writing applications which process 
 vast amounts of data in-parallel on large clusters 
 of commodity hardware in a reliable, fault-tolerant manner. 


In our project we will be running MapReduce on two varied datasets, one for the prototype and second for the final project deliverable. 

The first dataset, used for the prototype, is Complete History of Major League Baseball Stats from 1871 to 2015. This data set contains Major League Baseball’s complete batting and pitching statistics from 1871 to 2015, plus fielding statistics, standings, team stats, park stats, player demographics, managerial records, awards, post-season data, and more. We will run the MapReduce job on this dataset to calculate the average error at each fielding position.

### Steps to install and configure Hadoop cluster 

*	Install a Hadoop cluster, which typically involves unpacking the software(`Hadoop`) on all the machines in the cluster.

*	Create and set up `SSH` certificates, which will be used by Hadoop to communicate between machines in the cluster.

*	 Edit the configuration files – `core-default.xml`, `hdfs-default.xml`, `yarn-default.xml`, `mapred-default.xml`,`etc/hadoop/core-site.xml`, `etc/hadoop/hdfs-site.xml`, `etc/hadoop/yarn-site.xml` and `etc/hadoop/mapred-site.xml`, according to the requirements.

*	To configure the environment in which the Hadoop daemons execute as well as the configuration parameters for the Hadoop daemons use `etc/hadoop/hadoop-env.sh` and `etc/hadoop/yarn-env.sh`. 

   Starting the cluster by running following commands on Master node: -

*	 Formatting Namenode:


```
$	bin/hadoop namenode -format
```

*	Starting Namenode, Datanode and Jobtracker:

```
$	bin/start-dfs.sh
```

*	Starting Tasktracer:

```
$	bin/start–mapred.sh
```

*	Check if Hadoop started as desired using `jps` command.


### Steps to run MapReduce job
Once the Hadoop cluster is created and configured,

*	Set up the environment variables – `JAVA_HOME`, `PATH`, `HADOOP_CLASSPATH`.

*	Copy data from `local` to `hdfs` (Hadoop Distributed FileSystem) –


```
$	bin/hadoop fs -copyFromLocal <localsrc> <dest>
```

*	Compile and create a jar from the MapReduce.java file, which contains the code for mapper and reducer. 

```
$   bin/hadoop com.sun.tools.javac.Main MapReduce.java
```

```
$	jar cf ep.jar MapReduce *.class
```

*	Run the application: 

```
$	bin/hadoop jar mr.jar MapReduce <input_dir> <output_dir>
```

*	Output:

```
$	bin/hadoop fs -cat <output_dir/part-r-00000>
```


### Commands to be executed to run the protoype :


```
$ 	bin/hadoop namenode – format
```
```
$	bin/start-dfs.sh
```
```
$	bin/hadoop fs -mkdir /app/tmp/data
```
```
$	bin/hadoop fs -copyFromLocal /app/nba_data /app/tmp/data
```
```
$	bin/hadoop com.sun.tools.javac.Main ErrorPos.java
```
```
$	jar cf ep.jar ErrorPos *.class
```
```
$	bin/hadoop jar ep.jar ErrorPos /app/tmp/data /app/tmp/output
```
```
$	bin/hadoop fs -cat /app/tmp/output /part-r-00000>
```

### Useful Links

* http://hadoop.apache.org/docs/stable/index.html

* http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html

* http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html