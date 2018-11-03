#!/bin/bash

# SuccessRate

hadoop fs -mkdir PartC
hadoop fs -mkdir PartC/SuccessRate
hadoop fs -mkdir PartC/SuccessRate/Java
hadoop fs -mkdir PartC/SuccessRate/Java/input

hadoop fs -put PartC/SuccessRate/Java/input/buys.txt PartC/SuccessRate/Java/input/clicks.txt PartC/SuccessRate/Java/input
hadoop jar SuccessRate.jar successRatePartC.successRatePartC.successRate PartC/SuccessRate/Java/input/buys.txt PartC/SuccessRate/Java/input/clicks.txt PartC/SuccessRate/Java/output

