
# Setting up a cluster node

- Intall Ubuntu

# Install Spark

- Download a packaged release from https://spark.apache.org/downloads.html (e.g. spark-2.2.1-bin-hadoop2.7.tgz)
- Untar it
- Rename conf/spark-env.sh.template to conf/spark-env.sh and edit it to add SPARK_LOCAL_IP and SPARK_MASTER_HOST env vars 

## Run the master

Only one master is needed in the cluster.

```bash
$ nohup ./sbin/start-master.sh &
```

Make a note of the master URL.

## Run a slave

Run the slave, specifying the master URL noted in the previous step.

```bash
$ nohup ./sbin/start-slave.sh spark://192.168.0.37:7077 &
``` 

# Install DataFusion

TBD

