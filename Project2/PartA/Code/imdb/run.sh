#!/bin/bash


#remove locally existing output directory from previous runs
rm -f PartA/imdb/output.txt

#run the spark job
spark-submit PartA/Code/imdb/partA.py PartA/Code/imdb/quotes.list | tee PartA/imdb/output.txt
