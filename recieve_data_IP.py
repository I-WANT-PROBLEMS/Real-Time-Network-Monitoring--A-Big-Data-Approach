# # #readStream

ip_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","127.0.0.1:9092").option("subscribe","IP").load()
ip_df = ip_df.selectExpr("CAST(value AS STRING)")

# k = k.split(',')
# from pyspark.sql import SparkSession as spark
# import pandas as pd
import datetime

# timestamp = 1578326400022
# dt = datetime.datetime.fromtimestamp(timestamp / 1000.0)
# print(dt.strftime('%Y-%m-%d %H:%M:%S'))

# tmp = "1578326400001,13.43.52.51,18.70.112.62,40,57354"
from pymongo import MongoClient
import datetime
def process_df_IP(k):
    # k = k.split(',')
    k = list(k)
    # k = [str(i) for i in k]
    # print(type(k))
    client = MongoClient('mongodb://localhost:27017/')
    mongo_db = client["project"]
    db_collection = mongo_db["network_ip"]
    temp = k[0]
    temp = temp.split(',')
    ip1 = temp[1]
    ip2 = temp[2]
    ip1 = ip1.split('.')
    ip2 = ip2.split('.')
    k11 = str(ip1[0])
    k12 = str(ip1[1])
    k21 = str(ip2[0])
    k22 = str(ip2[1])
    ip1 = k11.replace("'","").replace(" ","") + '.' + k12.replace("'","").replace(" ","")
    ip2 = k21.replace("'","").replace(" ","") + '.' + k22.replace("'","").replace(" ","")
    temp[1] = ip1
    temp[2] = ip2
    temp[4] = int(temp[4])
    # print(temp)
    # print({"time_stamp":str(datetime.datetime.fromtimestamp(int(temp[0]) / 1000.0))[:19],"s_ip":temp[1],"d_ip":temp[2],"port":temp[3],"bytes":int(temp[4])})
    data = db_collection.insert_one({"time_stamp":str(datetime.datetime.fromtimestamp(int(temp[0]) / 1000.0))[:19],"s_ip":temp[1],"d_ip":temp[2],"port":temp[3],"bytes":int(temp[4])})

# ip_stream = ip_df.writeStream.foreach(process_df_IP).start()

# df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","127.0.0.1:9092").option("subscribe","test").load()
# df = df.selectExpr("CAST(value AS STRING)")
# k = df.writeStream.foreach(process_df).start()


# from pymongo import MongoClient
# client = MongoClient('mongodb://localhost:27017/')
# mongo_db = client["project"]
# db_collection = client["network"]


# def send_each_row(df, e_id):
#     df.show()
#     df.write("mongo")\
#         .mode("append")\
#         .option("database","project")\
#         .option("collection","network")\
#         .option("uri","mongodb://127.0.0.1:27017/")\
#         .save()