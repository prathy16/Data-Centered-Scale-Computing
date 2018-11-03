#!/bin/bash

# gutenberg

hadoop fs -mkdir PartB
hadoop fs -mkdir PartB/Java
hadoop fs -mkdir PartB/Java/gutenberg

hadoop jar wordcount.jar wordCount2000.wordCount2000.wordCount2000 s3://csci5253-gutenberg-dataset/ PartB/Java/gutenberg/output

