#
# producer.py
# June 07, 2020
# monitor system stats like CPU/memory and publish to Kafka topic 
#

import socket
import psutil

from datetime import datetime 
from time import sleep
from json import dumps
from kafka import KafkaProducer

import pandas as pd
import getpass

from kafka import KafkaConsumer
from json import loads
from mstrio.microstrategy import Connection
from mstrio.dataset import Dataset

producer = KafkaProducer(bootstrap_servers=['192.168.56.101:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

#create empty df
df_system_cpu = pd.DataFrame(columns = ['date_time','hostname','ip_address','timestamp','cpu','mem_avail'])

df_system_cpu = df_system_cpu.astype({'date_time': 'datetime64', 'hostname': 'object','ip_address':'object','timestamp':'object','cpu':'float64','mem_avail':'float64'})


#connect to mstr & create dataset
mstr_username = "mstr"
mstr_password = "Qc4cXuRrBYCK"
base_url = 'https://env-175743.customer.cloud.microstrategy.com/MicroStrategyLibrary/api'
login_mode = 1
project_id = 'B7CA92F04B9FAE8D941C3E9B7E0CD754'

conn = Connection(base_url, mstr_username, mstr_password, project_id=project_id, login_mode=login_mode)
conn.connect()

ds = Dataset(connection=conn, name="system_monitor_cube")
ds.add_table(name="readings", data_frame=df_system_cpu, update_policy="Add")
ds.create(folder_id="E59B6FE611EA21D4EA960080EFC58828")
print(ds.dataset_id)

#spool readings
for e in range(100000):

    #set up variables
    timestamp = datetime.now().isoformat()
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cpu = psutil.cpu_percent(interval=None, percpu=False)
    mem_avail = psutil.virtual_memory().available
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    
    #create msg
    msg = {'date_time' : date_time, 'hostname' : hostname, 'id_address' : ip_address, 'timestamp' : timestamp, 'cpu' : cpu, 'mem' : mem_avail}

    print(msg)

    #publish msg 
    producer.send('system_monitor', value=msg)
    
    sleep(1)