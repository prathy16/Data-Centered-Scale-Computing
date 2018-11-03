#!/bin/bash


#remove locally existing output directory from previous runs
rm -f PartA/holmes/output.txt

#run the spark job
spark-submit PartA/Code/holmes/partA.py PartA/Code/holmes/big.txt | tee PartA/holmes/output.txt
