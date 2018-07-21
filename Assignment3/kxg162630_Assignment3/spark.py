from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json 
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob
from nltk import tokenize
from elasticsearch import Elasticsearch

TCP_IP = 'localhost'
TCP_PORT = 9002

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

#Defining GetSentiment method
def getSentiment(t):
    analysis = TextBlob(t)
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

#Defining GetData method
def getJSONdata(x):
	jsonData = json.loads(x)
	sentiment = getSentiment(jsonData['text'])
	jsonData['sentiment'] = sentiment
	
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
	es.index(index='gun-index-elastic', doc_type='data',  body=json.dumps(jsonData))
	return jsonData


data = dataStream.map(lambda x : getJSONdata(x))
data.pprint()

ssc.start()
ssc.awaitTermination()

