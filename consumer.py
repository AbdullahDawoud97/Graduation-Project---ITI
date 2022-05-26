# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer#,TopicPartition

# Import sys module
import sys

# Import json module to serialize data
import json

import re
import pandas as pd
# Initialize consumer variable and set property for JSON decode
consumer = KafkaConsumer ('trump',value_deserializer=lambda m: json.loads(m.decode('utf-8')))



# Read data from kafka
for message in consumer:
    
    text=message[6]
    keys=list(pd.read_csv('Keys.csv',names=['key']).iloc[-1])[0]
    filename='tweets_'+str(keys)+'.csv'
    with open(filename, 'a') as tf:
        tf.write(str(text))
        tf.close()
    
# Terminate the script
#sys.exit()
