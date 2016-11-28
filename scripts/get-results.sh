#!/bin/bash

/opt/spark-2.0.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class com.bravolt.boss2016.ResultsApp --conf 'spark.driver.extraJavaOptions=-Dhdfs.url=hdfs://master:54310/votes/*' /opt/boss_voting_machine/spark/Result/target/scala-2.11/bravolt.jar
