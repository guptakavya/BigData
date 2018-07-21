from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

sc.stop()
conf = SparkConf().setMaster("local").setAppName("Question2")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

# Load and parse the data
data = sc.textFile("dbfs:/FileStore/tables/ratings.dat").map(lambda line: line.split("::")).map(lambda x: Rating(int(x[0]), int(x[1]), int(x[2])))
splits = data.randomSplit([6, 4], 24)
trainData = splits[0]
testData = splits[1]
rank = 10
iterations = 20
model = ALS.train(trainData, rank, iterations)

testLabel = testData.map(lambda p: ((p[0], p[1]), p[2]))
testData = testData.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testData).map(lambda r: ((r[0], r[1]), r[2]))

combined_result = predictions.join(testLabel)
MSE = combined_result.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))