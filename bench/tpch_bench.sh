#!/bin/bash

./bin/spark-submit --class org.apache.spark.examples.sql.dex.TPCHDataGen --master local[*] --conf spark.jars=third_party/postgresql-42.0.0.jar  ./examples/target/scala-2.11/jars/spark-examples_2.11-2.4.0.jar
