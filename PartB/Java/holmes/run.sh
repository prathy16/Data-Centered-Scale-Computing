#!/bin/bash

# holmes

hadoop fs -mkdir PartB
hadoop fs -mkdir PartB/Java
hadoop fs -mkdir PartB/Java/holmes
hadoop fs -mkdir PartB/Java/holmes/input

hadoop fs -put PartB/Java/holmes/input/big.txt PartB/Java/holmes/input
hadoop jar wordcount.jar wordCount.wordCount.wordCount PartB/Java/holmes/input/big.txt PartB/Java/holmes/output
