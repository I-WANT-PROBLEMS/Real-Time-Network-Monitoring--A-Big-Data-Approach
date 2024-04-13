# # #readStream

# from pyspark.sql import SparkSession as spark
import pandas as pd


from pymongo import MongoClient
mac_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","127.0.0.1:9092").option("subscribe","MAC").load()
mac_df = mac_df.selectExpr("CAST(value AS STRING)")


def process_df_MAC(k):
    k = list(k)
    client = MongoClient('mongodb://localhost:27017/')
    mongo_db = client["project"]
    db_collection = mongo_db["network_mac"]
    temp = k[0]
    temp = temp.split(',')
    for i in range(0,len(temp)):
        temp[i] = str(temp[i]).replace('"','').replace("'",'').replace(' ','')
    # print(temp)
    data = db_collection.insert_one({"time":temp[0],"source_mac":temp[1],"dest_mac":temp[2],"bytes":int(temp[3])})

# mac_streaming = mac_df.writeStream.foreach(process_df_MAC).start()

# client = MongoClient('mongodb://localhost:27017/')
# mongo_db = client["project"]
# db_collection = client["network"]