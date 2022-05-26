import numpy as np
import pandas as pd
from flask import Flask, request, jsonify, render_template
import pickle
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
from kafka import KafkaProducer
import time
from sentence_transformers import SentenceTransformer
import re

def cleanTxt(text):
    text = re.sub('@[A-Za-z0-9\_]+','',str(text))  #removed @mentions
    text = re.sub(r'#', '', text) #Removed the '#' symbol
    text = re.sub(r'-', '', text) #Removed the '-' symbol
    text = re.sub(r'RT[\s]+','',text) #Removed RT
    text = re.sub(r'http\S+', '', text) #Remove the hyper link
    text = re.sub(r'<.*?>','', text) # remove the html elements
    text = re.sub(r'\s+', ' ', text).strip()
    return text





access_token = "852941759281864708-3YPSoWoRNh2UCt7PrPtU4G5FT9nIREr"
access_token_secret =  "IF02EXLLaw7o9jDUUSId1AYz0m9eMCo4bRo7DoNknUyvs"
api_key =  "FOU1Arydm8RQKnaAlOc7vqALA"
api_secret =  "VdYtiYXYXwBNkHQaekwB6L3dRfAoRiz6ivMNtD6aBWbxbBVb9P"


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


app = Flask(__name__)
model = pickle.load(open('LR_Trained.pkl', 'rb'))

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))


@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict',methods=['POST'])
def predict():
    '''
    For rendering results on HTML GUI
    '''
    search_key = [str(x) for x in request.form.values()][0]
    with open('Keys.csv', 'a') as tf:
        tf.write(str(search_key)+"\n")
        tf.close()
    keys=list(pd.read_csv('Keys.csv',names=['key']).iloc[-1])[0]

   
    l = StdOutListener()
    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=keys,languages=["en"])
    
    filename='tweets_'+str(keys)+'.csv'
    df=pd.read_csv(filename,delimiter=':;,',names=('name','tweet_text'))
    df['clean'] = df['tweet_text'].apply(cleanTxt)
    



    sbert_model = SentenceTransformer('bert-base-nli-mean-tokens')

    sent_vec=sbert_model.encode(df['tweet_text'],batch_size = 32,show_progress_bar=True)

    p=model.predict_proba(sent_vec)[:,1]
    df['kind']=(-1*(p<0.4))+(p>0.65)


    pos=tuple(df[df['kind']==1]['tweet_text'])
    neu=tuple(df[df['kind']==0]['tweet_text'])
    neg=tuple(df[df['kind']==-1]['tweet_text'])



    return render_template('pre.html', pos=pos,neg=neg,neu=neu)




@app.route('/predict_api',methods=['POST'])
def predict_api():
    '''
    For direct API calls trought request
    '''
    data = request.get_json(force=True)
    prediction = model.predict([np.array(list(data.values()))])

    output = prediction[0]
    return jsonify(output)

if __name__ == "__main__":
    app.run(debug=True)
