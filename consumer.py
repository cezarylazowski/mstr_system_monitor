#
# consumer.py
# June 07, 2020
# monitor system stats like CPU/memory and publish to Kafka topic
#

import pandas as pd
import getpass

from kafka import KafkaConsumer
from json import loads
from mstrio.microstrategy import Connection
from mstrio.dataset import Dataset

#connect to mstr
mstr_username = "mstr"
mstr_password = "Qc4cXuRrBYCK"
base_url = 'https://env-175743.customer.cloud.microstrategy.com/MicroStrategyLibrary/api'
login_mode = 1
project_id = 'B7CA92F04B9FAE8D941C3E9B7E0CD754'

conn = Connection(base_url, mstr_username, mstr_password, project_id=project_id, login_mode=login_mode)
conn.connect()

#kafka connection
consumer = KafkaConsumer(
    'system_monitor',
     bootstrap_servers=['192.168.56.101:9092'],
     auto_offset_reset='last',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

#set up df, counter
df_system_cpu = pd.DataFrame(columns = ['timestamp'])
counter = 0
 

#read msg, append to df; every 10th msg write df to MSTR and flush df
for message in consumer:

    counter += 1
    message = message.value
    df_system_cpu = df_system_cpu.append(message, ignore_index=True)

    print(counter)
    print(message)

    if counter % 10 == 0:

        print(df_system_cpu)
        print('writing to cube...')

        ds = Dataset(connection=conn, name="system_monitor_cube")
        ds.add_table(name="readings", data_frame=df_system_cpu, update_policy="Add")
        ds.create(folder_id="E59B6FE611EA21D4EA960080EFC58828")
        df_system_cpu = df_system_cpu[0:0]