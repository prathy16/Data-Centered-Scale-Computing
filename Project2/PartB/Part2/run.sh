#!/bin/bash


#remove locally existing output directory from previous runs
rm -f PartB/Part2/Bucket1/output.txt
rm -f PartB/Part2/Bucket2/output.txt
rm -f PartB/Part2/Bucket3/output.txt
rm -f PartB/Part2/Bucket4/output.txt
rm -f PartB/Part2/Bucket5/output.txt

#run the spark job
spark-submit --master local[3] --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 PartB/Part2/PartB_2.py
