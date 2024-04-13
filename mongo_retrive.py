from pymongo import MongoClient
import pandas as pd
import seaborn as sns
import streamlit as st
import plotly.express as pye
from streamlit_autorefresh import st_autorefresh

st.set_page_config(layout="wide",page_icon="random",page_title="Band-Width and Network Monitoring")
st.markdown("<h1 style='text-align: center; color: Yellow;' > Team-20 </h1>",unsafe_allow_html=True)
st.markdown("<h1 style='text-align: center; color: gold;' > Network Monitor👨‍💻 </h1>",unsafe_allow_html=True)
if st.checkbox("live"):
    st_autorefresh(2*1000)
client = MongoClient('mongodb://localhost:27017/')
mongo_db = client["project"]
db_collection = mongo_db["network_ip"]

# d_list = []
# for i in data:
#     time_stamp = i["time_stamp"]
#     source_ip = i["s_ip"]
#     dest_ip = i["d_ip"]
#     port = i["port"]
#     bytes_ = i["bytes"]
#     d_list.append([time_stamp,source_ip,dest_ip,port,bytes_])
# dataframe = pd.DataFrame(d_list, columns=["time_stamp","source_ip","dest_ip","port","bytes"])

# while True:
data = db_collection.find({})
df = pd.DataFrame(list(data))

# groupby df using s_ip
df = df.dropna(how="any")
df1 = df.drop(["_id","time_stamp","d_ip","port"],axis=1)
df1['s_ip'] = df1['s_ip'].astype(str)
# df1.info()

grouped_df = df.groupby("s_ip").sum().reset_index()
# st.write(grouped_df.tail(1))
# st.write(m_ip)
# grouped_df = grouped_df.rename(columns={0: 's_ip', 1: 'bytes'})
# grouped_df.to_csv("temp.csv")


# data = db_collection.find({})
# df = pd.DataFrame(list(data))
# print(df.count())
# print(df.info())
# columns = ["_id","time_stamp","s_ip","d_ip","port","bytes"]
df1 = grouped_df[grouped_df['bytes'] > (grouped_df['bytes'].mean())*3]
df1['bytes'] = df1['bytes'].div(1024*1024)
df1 = df1.sort_values(by='bytes')

# st.write(sns.barplot(x='s_ip',y='bytes',data=df1))
st.markdown("<h3 style='text-align: center; color: cyan;' > IP Monitor </h3>",unsafe_allow_html=True)

st.line_chart(df1,x='s_ip',y='bytes')
st.bar_chart(df1,x='s_ip',y='bytes')
count = df.count()
df_subs = df[::1000]
st.markdown("<h3 style='text-align: center; color: cyan;' > Time-Series Monitor </h3>",unsafe_allow_html=True)

st.line_chart(df_subs,x=['time_stamp'],y=['bytes'])
# fig = pye.box(df1,x='s_ip',y='bytes')

# st.plotly_chart(fig)
# m_ip = df.values[0][2]

m_ip = grouped_df[grouped_df['bytes'] == grouped_df['bytes'].max()]
m_ip = m_ip['s_ip'].values[0]

# x = df[df['s_ip']==m_ip]['d_ip']
x = df.loc[df['s_ip']==m_ip,['d_ip','bytes']]

x_1 = x.groupby("d_ip").sum().reset_index()
x_1['bytes'] = x_1['bytes'].div(1024*1024)

x_1 = x_1[x_1['bytes'] > (x_1['bytes'].mean())*2.5]

x_1 = x_1.sort_values(by='bytes')

st.markdown("<h3 style='text-align: center; color: cyan;' > Top-IP Monitor</h3>",unsafe_allow_html=True)
st.bar_chart(x_1,x='d_ip',y='bytes')



client1 = MongoClient('mongodb://localhost:27017/')
mongo_db1 = client1["project"]
db_collection1 = mongo_db1["network_mac"]

mac_data = db_collection1.find({})
mac_df = pd.DataFrame(list(mac_data))
mac_df = mac_df.fillna(method='ffill')


mac_df1 = mac_df.groupby("source_mac").sum().reset_index()
mac_df1 = mac_df1[mac_df1['bytes'] > (mac_df1['bytes'].mean())*3]
mac_df1['bytes'] = mac_df1['bytes'].div(1024*1024)
mac_df1 = mac_df1.sort_values(by='bytes')


st.markdown("<h3 style='text-align: center; color: cyan;' > MAC Monitor </h3>",unsafe_allow_html=True)
st.line_chart(mac_df1,x="source_mac",y="bytes")
st.bar_chart(mac_df1,x="source_mac",y="bytes")

m_mac = mac_df[mac_df['bytes'] == mac_df['bytes'].max()]
m_mac = m_mac['source_mac'].values[0]
# st.write(m_mac)

x_mac = mac_df.loc[mac_df['source_mac']==m_mac,['dest_mac','bytes']]
x_mac = x_mac.groupby("dest_mac").sum().reset_index()
x_mac['bytes'] = x_mac['bytes'].div(1024*1024)

st.markdown("<h3 style='text-align: center; color: cyan;' > Top-MAC Monitor </h3>",unsafe_allow_html=True)
st.bar_chart(x_mac,x='dest_mac',y='bytes')

# st.map(mac_df1.tail(5))
top_MAC = mac_df1.tail(5)['source_mac']
top_MAC = list(set(top_MAC.to_list()))
# print(list(set(top_MAC)))
# for i in mac_df1.values[1]:
#     st.selectbox(i)
st.write("Keep an eye on these devices: ")
for i in top_MAC:
    if i == "nan":
        continue
    else:
        st.markdown("""
        - {i}
        """.format(i=i))