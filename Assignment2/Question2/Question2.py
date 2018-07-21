# Databricks notebook source
from pyspark import SparkContext
import heapq
import sys

# Define SparkContext
sc.stop()
sc = sc = SparkContext('local', 'Question 2') 

arguments = sys.argv

lines = sc.textFile("dbfs:/FileStore/InputForQuestion2/part-00000")

lists = lines.map(lambda x: x.split("	")).map(lambda x: (str(x[0]), int(x[1])))

topTenList = sc.parallelize(lists.takeOrdered(10, key = lambda x: -x[1]))

friendsTopTenList = topTenList.flatMap(lambda x: [(int(i),str(x[0]) + "-" + str(x[1])) for i in x[0].split(",")])

details = sc.textFile("dbfs:/FileStore/tables/userdata.txt").map(lambda x: x.strip().split(",")).map(lambda x: (int(x[0]), x[1:]))

joined_rdd = friendsTopTenList.join(details).map(lambda x: (x[1][0], x[1][1])).reduceByKey(lambda x,y: (x,y))

result = joined_rdd.map(lambda x: str(x[0].split("-")[1]) + "\t" + x[1][0][0] + "\t" + x[1][0][1] + "\t" + x[1][0][2] + "\t" + x[1][0][3] + "\t" + x[1][0][4] + "\t" + x[1][0][5] + "\t" + x[1][0][6] + "\t" + x[1][0][7] + "\t" + x[1][0][8] + "\t" + x[1][1][0] + "\t" + x[1][1][1] + "\t" + x[1][1][2] + "\t" + x[1][1][3] + "\t" + x[1][1][4] + "\t" + x[1][1][5] + "\t" + x[1][1][6] + "\t" + x[1][1][7] + "\t" + x[1][1][8]).repartition(1)

result.saveAsTextFile("dbfs:/FileStore/tables/Question2Outputnew")

# COMMAND ----------


