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

#connection to MSTR dataset
ds = Dataset(connection=conn, dataset_id="AB23CF8811EAAAB59F310080EF150B0C")
print(ds.dataset_id)


#kafka connection
consumer = KafkaConsumer(
    'system_monitor',
     bootstrap_servers=['192.168.56.101:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

#create df - recreate dataset/cube
df_system_cpu = pd.DataFrame(columns = ['date_time','hostname','ip_address','timestamp','cpu_count','cpu_cores','cpu_core_1','cpu_core_2','vm_avail','vm_used','swap_memory','disk_io_read','disk_io_write','net_io_sent','net_io_recv'])

df_system_cpu = df_system_cpu.astype({'date_time': 'datetime64', 'hostname': 'object','ip_address':'object','timestamp':'object','cpu_count': 'float64','cpu_cores': 'float64','cpu_core_1': 'float64','cpu_core_2': 'float64','vm_avail': 'float64','vm_used': 'float64','swap_memory': 'float64','disk_io_read': 'float64','disk_io_write': 'float64','net_io_sent': 'float64','net_io_recv': 'float64'})

#counter
counter = 0

#read msg, append to df; every 10th msg write DF to MSTR and flush
for message in consumer:

    counter += 1
    message = message.value
    df_system_cpu = df_system_cpu.append(message, ignore_index=True)
    print(message)

    if counter % 5 == 0:

        print(df_system_cpu.to_string())
        print('writing to cube...')

        ds.add_table(name="readings", data_frame=df_system_cpu, update_policy="add")
        ds.update()
        ds.publish()

