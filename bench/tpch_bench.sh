#!/bin/bash

./bin/spark-submit --master local[*] \
                   --jars third_party/postgresql-42.0.0.jar,third_party/Clusion-1.0-SNAPSHOT.jar \
                   --driver-class-path third_party/postgresql-42.0.0.jar:third_party/Clusion-1.0-SNAPSHOT.jar \
                   --conf spark.executor.extraClassPath=third_party/postgresql-42.0.0.jar:third_party/Clusion-1.0-SNAPSHOT.jar \
                   --class org.apache.spark.examples.sql.dex.TPCHDataGen ./examples/target/scala-2.11/jars/spark-examples_2.11-2.4.0.jar


/bin/spark-submit --master local[*] \
                  --jars third_party/postgresql-42.0.0.jar,third_party/Clusion-1.0-SNAPSHOT.jar                      --driver-class-path third_party/postgresql-42.0.0.jar:third_party/Clusion-1.0-SNAPSHOT.jar \
                  --conf spark.executor.extraClassPath=third_party/postgresql-42.0.0.jar:third_party/Clusion-1.0-SNAPSHOT.jar \
                  --driver-memory 12g \
                  --conf spark.driver.maxResultSize=0 \
                  --class org.apache.spark.examples.sql.dex.TPCHDataGen ./examples/target/scala-2.11/jars/spark-examples_2.11-2.4.0.jar standalone all
