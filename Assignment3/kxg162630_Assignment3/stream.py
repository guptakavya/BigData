import json 
import tweepy
import socket
import re
import requests

#Removing Links from the data
def removingLinks(t):
    t = re.sub(r"http\S+", "", t)
    return t

#Removing Emojis from the data
def removingEmoji(t):
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"
        u"\U0001F300-\U0001F5FF"
        u"\U0001F680-\U0001F6FF"
        u"\U0001F1E0-\U0001F1FF"
                           "]+", flags=re.UNICODE)
    t = emoji_pattern.sub(r'', t)
    return t

#Removing white space characters from the data    
def removingSpaces(t):
    t = re.sub(r'@\S*[\s, .]',r'',t)
    return t

#Removing Special Characters
def removingSpecialCharacters(tweet): 
    tweet = tweet.replace("RT", "")
    tweet = re.sub(r'[^a-zA-Z0-9 .#]',r'',tweet)
    return tweet

#Access Tokens and Keys
ACCESS_TOKEN = '984486545041313793-12d5AlbRPPaZBlkzlLSl0uBfzD13OnI'
ACCESS_SECRET = 'xuLWnsVKxgsinTPGf7ARO4kPu4IEwKz6j9w0S1nQrHlP3'
CONSUMER_KEY = 'Dfeyuzv6U4inGQrKLbVxqbWmV'
CONSUMER_SECRET = '	sMJqc7ZVlVb6RfhwrM8Cfhdnmsk06A3wNh8Kjiq8dCQhN2qTT5'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

hashtag = '#guncontrolnow'
TCP_IP = 'localhost'
TCP_PORT = 9002

#Creating sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):
    
    def on_status(self, status):    

        tweet = status.text
        tweet = removingLinks(tweet)
        tweet = removingSpaces(tweet)
        tweet = removingEmoji(tweet)
        tweet = removingSpecialCharacters(tweet)

        loc =  status.user.location
        if loc is None:
            return { 'lat' : 0.0, 'lon' : 0.0}
        else:
            params = {'sensor': 'false', 'address': loc}
            r = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=params)
            results = r.json()['results']
            for res in results:
                print(res['geometry']['location'])
                location = { 'lat' :res['geometry']['location']['lat'],
                         'lon' : res['geometry']['location']['lng']}

        dict = {'text': tweet, 
                'location': loc, 
                'timestamp_ms' : status.timestamp_ms }
        print(dict)
        conn.send((json.dumps(dict) + "\n").encode('utf-8'))
    
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag])
