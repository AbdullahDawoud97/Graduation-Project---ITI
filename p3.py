from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
from kafka import KafkaProducer
import time
import pandas as pd




producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))


access_token = "1462636987119292420-W3brhAaAn0KRgTQCVKZjQpj2UkdvEo"
access_token_secret =  "Mv7wSy0BhJCKZNqeBNS0xcKkUKPGIQGEfnUfQTrtROhFj"
api_key =  "wy2qiiTINwPkPDJlaIaCYoYA9"
api_secret =  "Dzd0Zh3ubhvoXrT8zqGGC7EqdMg4LpWWqQjOZusyMJtbyXJcCo"


class StdOutListener(StreamListener):
    def __init__(self, time_limit=1):
        self.start_time = time.time()
        self.limit = time_limit
        super(StdOutListener, self).__init__()

    
    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            json_text = json.loads(data) 
            final_row=""
            if('user' in json_text):
                final_row+=str(json_text['user']['name'])+':;,'
            else :
                final_row+="---"+":;,"
            final_row=str(json_text['user']['name'])+':;,'
            if('retweeted_status' in json_text):
                json_text=json_text['retweeted_status']
            if('extended_tweet' in json_text):
                final_row+=str(json_text['extended_tweet']['full_text']).replace("\n", ' ')+'\n'
            else :
                final_row+=str(json_text['text']).replace("\n", ' ')+'\n'
            
            producer.send("trump", value=final_row)
            return True
        else:
            return False
    
    def on_error(self, status):
        print (status)
keys=list(pd.read_csv('Keys.csv',names=['key']).iloc[-1])[0]
print(keys)
l = StdOutListener()
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=keys,languages=["en"])
    
