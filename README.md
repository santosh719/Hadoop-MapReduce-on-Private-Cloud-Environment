# Hadoop MapReduce on Private Cloud Environment
 
 
   

File name | Purpose | New/Modified | Comments
------------- | ------------- | ---------- | --------
ErrorPos.java  | Program to find the error in each fielding position | New | Implemented on The History of Baseball Dataset 
FineFoodReviews.java  | Program to find most helpful users (users who have the most helpful reviews out of their total reviews) | New | Implemented on Amamzon Fine Food Reviews Dataset (1st Question)
PopularProduct.java  | Program to find most popular products among all. | New | Implemented on Amamzon Fine Food Reviews Dataset (2nd Question)
ActiveUser.java  | Program to find most active users | New | Implemented on Amamzon Fine Food Reviews Dataset (3rd Question)
The objective of this project is to implement a Hadoop MapReduce environment on an isolated private cloud or a private cloud environment. 

> `Hadoop MapReduce` is a software framework 
 for easily writing applications which process 
 vast amounts of data in-parallel on large clusters 
 of commodity hardware in a reliable, fault-tolerant manner. 


In our project we ran MapReduce jobs on two varied datasets, one for the prototype and second for the final project deliverable. 

The first dataset, used for the prototype, is Complete History of Major League Baseball Stats from 1871 to 2015. This data set contains Major League Baseball’s complete batting and pitching statistics from 1871 to 2015, plus fielding statistics, standings, team stats, park stats, player demographics, managerial records, awards, post-season data, and more. We ran the MapReduce job on this dataset to calculate the average error at each fielding position.

The second data, used for the final phase, is Amazon Fine Food Reviews. This dataset consists of 568,454 food reviews Amazon users left up to October 2012. We ran 3 MapReduce jobs to analyze the following questions -
1. The users who have the most helpful feedback.
2. The products which are most popular/most reviewed.
3. The users who most active/users with most number of reviews.

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
$	jar cf mr.jar MapReduce *.class
```

*	Run the application: 

```
$	bin/hadoop jar mr.jar MapReduce <input_dir> <output_dir>
```

*	Output:

```
$	bin/hadoop fs -cat <output_dir/part-r-00000>
```




### Useful Links

* http://hadoop.apache.org/docs/stable/index.html

* http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html

* http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html