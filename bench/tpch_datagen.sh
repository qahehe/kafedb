#!/bin/bash

set -e

if [[ $# < 2 ]]; then
	  echo "Usage: tpch_datagen.sh <dex_variant> data_mode [, data_mode]"
	  exit 123
fi

echo "dex variant=$1"
echo "reps=$2"

jar_jdbc=third_party/postgresql-42.0.0.jar
jar_bc=third_party/bcprov-jdk15on-164.jar

for i in $(eval echo {1..$reps}); do
	  echo "rep=$i"
	  ./bin/spark-submit --master local[*] \
			                 --jars $jar_jdbc,$jar_bc \
			                 --driver-class-path $jar_jdbc:$jar_bc  \
			                 --conf spark.executor.extraClassPath=$jar_jdbc:$jar_bc \
			                 --driver-memory 6g   \
			                 --conf spark.driver.maxResultSize=0 \
			                 --class org.apache.spark.examples.sql.dex.TPCHDataGen ./examples/target/scala-2.11/jars/spark-examples_2.11-2.4.0.jar \
			                 $@
done

echo "done"
