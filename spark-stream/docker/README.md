# spark

A `debian:jessie` based [Spark](http://spark.apache.org) container. Use it in a standalone cluster with the accompanying `docker-compose.yml`, or as a base for more complex recipes.

## docker example

To run `SparkPi`, run the image with Docker:

    docker run --rm -it -p 4040:4040 gettyimages/spark bin/run-example SparkPi 10

To start `spark-shell` with your AWS credentials:

    docker run --rm -it -e "AWS_ACCESS_KEY_ID=YOURKEY" -e "AWS_SECRET_ACCESS_KEY=YOURSECRET" -p 4040:4040 gettyimages/spark bin/spark-shell

To do a thing with Pyspark

    echo "import pyspark\nprint(pyspark.SparkContext().parallelize(range(0, 10)).count())" > count.py
    docker run --rm -it -p 4040:4040 -v $(pwd)/count.py:/count.py gettyimages/spark bin/spark-submit /count.py

## docker-compose example

To create a simplistic standalone cluster with [docker-compose](http://docs.docker.com/compose):

    docker-compose up

The SparkUI will be running at `http://${YOUR_DOCKER_HOST}:8080` with one worker listed. To run `pyspark`, exec into a container:

    docker exec -it dockerspark_master_1 /bin/bash
    bin/pyspark

To run `SparkPi`, exec into a container:

    docker exec -it dockerspark_master_1 /bin/bash
    bin/run-example SparkPi 10

## license

MIT


# Steps to atach debugger on spark container

    docker exec -it master_containner_name bin/bash
    export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_sock*__*et,server=y,suspend=y,address=4000

    bin/spark-submit --name FleetPerformanceMainInfrastructureSpark --class com.ceabs.fleetperformance.spark.FleetPerformanceInfrastructureSparkJob 
    --master local[2] /tmp/java/fleetperformance-infrastructure-1.0-DEV.jar /tmp/data/event  "not_matters_now"  "22"


Create a remote debugger on Intellij pointing to 4000 port and the ip from de master docker spark.

Change the debugger as atach and the transport as socket



# Resources
https://github.com/gettyimages/docker-spark
http://www.bigendiandata.com/2016-08-26-How-to-debug-remote-spark-jobs-with-IntelliJ/
http://danosipov.com/?p=779