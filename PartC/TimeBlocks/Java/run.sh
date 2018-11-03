#!/bin/bash

# TimeBlocks

hadoop fs -mkdir PartC
hadoop fs -mkdir PartC/TimeBlocks
hadoop fs -mkdir PartC/TimeBlocks/Java
hadoop fs -mkdir PartC/TimeBlocks/Java/input

hadoop fs -put PartC/TimeBlocks/Java/input/buys.txt PartC/TimeBlocks/Java/input
hadoop jar TimeBlocks.jar Mapreduce.MaxBuys.MaxBuys PartC/TimeBlocks/Java/input/buys.txt PartC/TimeBlocks/Java/output

