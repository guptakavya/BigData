# Databricks notebook source
from decimal import Decimal

def getRatings(rating):
  ratings = rating.split(',')
  if ratings[2] == 'rating':
    return (0, (0.0, 0))
  return (ratings[1], (Decimal(ratings[2]), 1))

def ratingObject(x, y):
  floatx = Decimal(x[0])
  floaty = Decimal(y[0])
  return (floatx + floaty, x[1]+y[1])

def getTag(tag):
  tagData = tag.split(',')
  if tagData[1] == 'movieId':
    return ('', '')
  return (tagData[1], tagData[2])

def getLowAverage(x):
  return str(str(x[0]) + '\t' + str('%.3f'%(x[1])))
  
def getThriller(x):
  return str(str(x[0]) + '\t' +  str('%.3f'%(x[1][0][0])) + '\t' + str(x[1][0][1]) + '\t' + str(x[1][1]))

def averageRating(rating):
  if rating[1][1] is 0:
    return (0, 0.0)
  sumTotal = Decimal(rating[1][0])
  count =  rating[1][1]
  average = sumTotal / count
  return (rating[0], average)

def getAverage(x):
  return str(str(x[0]) + '\t' + str('%.3f'%(x[1])))
  
moviesdata = spark.read.csv("dbfs:/FileStore/tables/movies.csv", header=True, mode="DROPMALFORMED")
movies = moviesdata.rdd.map(lambda x : (x[0], x[2]))
  
ratings = sc.textFile("dbfs:/FileStore/tables/ratings.csv").map(lambda x : getRatings(x))
ratingReduce = ratings.reduceByKey(lambda x, y : ratingObject(x, y))

#Finding the average movie rating 
average = ratingReduce.map(lambda rating : averageRating(rating))
average.collect()
averageData = average.map(lambda x : getAverage(x))
averageData.first()
averageData.saveAsTextFile("dbfs:/FileStore/AvgMovieRating/")

#finding the lowest average 
lowAverage = average.takeOrdered(10, key = lambda x: x[1])
lowAverageOrdered = sc.parallelize(lowAverage)
lowAverageData = lowAverageOrdered.filter(lambda x: x != (0, 0.0)).map(lambda x : getLowAverage(x))
lowAverageData.first()
lowAverageData.saveAsTextFile("dbfs:/FileStore/LowestAvg/")

#finding the movie with tag 'Action'
tags = sc.textFile("dbfs:/FileStore/tables/tags.csv").map(lambda x : getTag(x))
actionData = average.leftOuterJoin(tags).filter(lambda x : x[1][1] == 'action')
actionData.collect() 
actionDataCSV = actionData.map(lambda x : str(str(x[0]) + '\t' + str('%.3f'%(x[1][0])) + '\t' + str(x[1][1])))
actionDataCSV.saveAsTextFile("dbfs:/FileStore/MovieTagAction/")

#finding the movie with tag 'thriller'
thriller = actionData.leftOuterJoin(movies).filter(lambda x : 'Thriller' in x[1][1])
thriller.collect()
thrillerData = thriller.map(lambda x : getThriller(x))
thrillerData.saveAsTextFile("dbfs:/FileStore/GenreThrill/")

# COMMAND ----------


