import pandas as pd
from kafka import KafkaProducer
import numpy as np
import time
df = pd.read_csv('/home/jaswanth/Documents/BDA/Sem-5/Project/Data/Dataset_MAC.csv')

df1 = df.drop(["Info","Protocol","No."],axis=1)

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',key_serializer=str.encode,value_serializer=str.encode)

for row in df1.values:
    temp = np.ndarray.tolist(row)
    temp_ = str(temp).replace('[','').replace(']','')
    #print(type(temp_))
    producer.send("MAC",key='dummy2',value=temp_)
    time.sleep(0.07)