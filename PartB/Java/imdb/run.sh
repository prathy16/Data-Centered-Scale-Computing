#!/bin/bash

# imdb

hadoop fs -mkdir PartB
hadoop fs -mkdir PartB/Java
hadoop fs -mkdir PartB/Java/imdb
hadoop fs -mkdir PartB/Java/imdb/input

hadoop fs -put PartB/Java/imdb/input/quotes.list PartB/Java/imdb/input
hadoop jar wordcount.jar wordCount.wordCount.wordCount PartB/Java/imdb/input/quotes.list PartB/Java/imdb/output
