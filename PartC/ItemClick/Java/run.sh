#!/bin/bash

# ItemClick

hadoop fs -mkdir PartC
hadoop fs -mkdir PartC/ItemClick
hadoop fs -mkdir PartC/ItemClick/Java
hadoop fs -mkdir PartC/ItemClick/Java/input

hadoop fs -put PartC/ItemClick/Java/input/clicks.txt PartC/ItemClick/Java/input
hadoop jar ItemClick.jar MapReduce.MaxClicks.MaxClicks PartC/ItemClick/Java/input/clicks.txt PartC/ItemClick/Java/output
