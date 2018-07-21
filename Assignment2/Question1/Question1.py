# Databricks notebook source
import itertools
import sys 
from pyspark import SparkContext
# Define SparkContext
sc.stop()
sc = SparkContext('local', 'Question 1')
#Take input from input file 
lines = sc.textFile("dbfs:/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
#Defining list of friends 
lists = lines.map(lambda x: x.strip().split("	")).filter(lambda x: len(x) == 2).map(lambda x: [int(x[0]), sorted([int(i) for i in x[1].split(",")])])

pair = lists.flatMap(lambda x: [(i, x[1]) for i in [",".join( sorted([ str(x[0]), str(i) ]) ) for i in x[1]]])

result = pair.reduceByKey(lambda x,y: set(x).intersection(set(y))).map(lambda x: x[0] + "\t" + str(x[1]))

result.saveAsTextFile("dbfs:/FileStore/Question1")

result2 = pair.reduceByKey(lambda x,y: len(set(x).intersection(set(y)))).map(lambda x: x[0] + "\t" + str(x[1]))

result2.saveAsTextFile("dbfs:/FileStore/InputForQuestion2")

# COMMAND ----------


