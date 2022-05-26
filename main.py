import multiprocessing
import os                                                               
import time
from sentence_transformers import SentenceTransformer
import re
import numpy as np
import pandas as pd
from flask import Flask, request, jsonify, render_template
import pickle




def cleanTxt(text):
    text = re.sub('@[A-Za-z0-9\_]+','',str(text))  #removed @mentions
    text = re.sub(r'#', '', text) #Removed the '#' symbol
    text = re.sub(r'-', '', text) #Removed the '-' symbol
    text = re.sub(r'RT[\s]+','',text) #Removed RT
    text = re.sub(r'http\S+', '', text) #Remove the hyper link
    text = re.sub(r'<.*?>','', text) # remove the html elements
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def execute(process):
    os.system(f'python {process}')


app = Flask(__name__)
model = pickle.load(open('LR_Trained.pkl', 'rb'))


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

 
    # Creating the tuple of all the processes
    all_processes = ('p1.py', 'p2.py','p3.py')                                    
                                                                          
                                           
                                                                         
    process_pool = multiprocessing.Pool(processes = 3)                                                        
    process_pool.map(execute, all_processes)

   
    
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





