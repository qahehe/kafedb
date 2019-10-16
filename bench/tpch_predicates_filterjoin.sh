#!/bin/bash

set -e

dex_variant=$1
reps=${2:-1}

if [ -z $dex_variant ]; then
	  echo "Usage: tpch_predicates_filterjoin.sh <dex_variant> <reps>"
	  exit 123
fi

echo "dex variant=$1"
echo "reps=$2"

for i in $(eval echo {1..$reps}); do
	  echo "rep=$i"
	  ./bin/spark-submit --master local[*] \
			                 --jars third_party/postgresql-42.0.0.jar,third_party/Clusion-1.0-SNAPSHOT.jar  \
			                 --driver-class-path third_party/postgresql-42.0.0.jar:third_party/Clusion-1.0-SNAPSHOT.jar  \
			                 --conf spark.executor.extraClassPath=third_party/postgresql-42.0.0.jar:third_party/Clusion-1.0-SNAPSHOT.jar \
			                 --driver-memory 6g   \
			                 --conf spark.driver.maxResultSize=0 \
			                 --class org.apache.spark.examples.sql.dex.TPCHPredicatesFilterJoin ./examples/target/scala-2.11/jars/spark-examples_2.11-2.4.0.jar \
			                 $dex_variant \
			  &> /data/log-predicates-filterjoin-$dex_variant-$i
done

echo "done"
