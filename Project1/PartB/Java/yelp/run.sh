#!/bin/bash

# yelp

hadoop fs -mkdir PartB
hadoop fs -mkdir PartB/Java
hadoop fs -mkdir PartB/Java/yelp

hadoop jar wordcount.jar wordCount2000.wordCount2000.wordCount2000 s3://wordcount-datasets/ PartB/Java/yelp/output
