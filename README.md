##Purpose 
This is a repo for my blog post at the [Pythian bog](https://docs.google.com/a/pythian.com/document/d/12f0-YVjfZ8Q6gkT13Jh-Ml6d3ZkVzvWzh2Ugs0xdUq8/edit?usp=sharing)

##Prerequisites

Have docker, python 2.7 and  jre 1.7 installed, Scala and basic familiarity with Spark and the concept or RDD’s

## Setup

* Clone this repo.
* Create a virtualenv for this project `mkvirtualenv streaming_percentile` (optional)
* Install requirements using `pip install -r requirements.pip`
* Install the kafka docker container. If you have a kafka cluster set up you can skip this step
..* On mac `docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`boot2docker ip` --env ADVERTISED_PORT=9092 spotify/kafka`
..* On linux `docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`127.0.0.1` --env ADVERTISED_PORT=9092 spotify/kafka`
..* More info on the container can be found [here.](https://github.com/spotify/docker-kafka)
..* Download and extract the [kafka binaries](http://kafka.apache.org/downloads.html). This location will now be refered to as KAFKA_HOME.
* Install Spark
..* THis can either be run locally or using a docker container
..* To run locally Download the binaries [here](https://spark.apache.org/downloads.html)
..* `wget http://apache.claz.org/spark/spark-1.4.0/spark-1.4.0-bin-hadoop2.6.tgz`
..* `tar -xvf spark-1.4.0-bin-hadoop2.6.tgz`
..* Run the pyspark shell to confirm using `./bin/pyspark`
..* You can also run this with IPython if you have IPython installed using `IPYTHON=1 ./bin/pyspark`
..* You can also run this as a docker container using `docker run -i -t -h -p 8888:8888 -v my_code:/app sandbox anantasty/ubuntu_spark_ipython:1.0
 bash`


