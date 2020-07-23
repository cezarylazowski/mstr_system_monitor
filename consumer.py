#
# consumer.py
# June 07, 2020
# monitor system stats like from kafa stream and publish to MSTR cube
# command line parameters:
#     python3 consumer.py reset     -- cube reset
#     python3 consumer.py noreset   -- no cube reset
#

import sys
import pandas as pd
import getpass

from kafka import KafkaConsumer
from json import loads
from mstrio.microstrategy import Connection
from mstrio.dataset import Dataset

#create df - recreate dataset/cube
df_system_cpu = pd.DataFrame(columns = ['date_time','hostname','ip_address','timestamp','cpu_count','cpu_cores','cpu_core_1','cpu_core_2','vm_avail','vm_used','swap_memory','disk_io_read','disk_io_write','net_io_sent','net_io_recv'])

df_system_cpu = df_system_cpu.astype({'date_time': 'datetime64', 'hostname': 'object','ip_address':'object','timestamp':'object','cpu_count': 'float64','cpu_cores': 'float64','cpu_core_1': 'float64','cpu_core_2': 'float64','vm_avail': 'float64','vm_used': 'float64','swap_memory': 'float64','disk_io_read': 'float64','disk_io_write': 'float64','net_io_sent': 'float64','net_io_recv': 'float64'})


#connect to mstr
mstr_username = "######"
mstr_password = "######"
base_url = 'https://env-######.customer.cloud.microstrategy.com/MicroStrategyLibrary/api'
login_mode = 1
project_id = 'B7CA92F04B9FAE8D941C3E9B7E0CD754'

conn = Connection(base_url, mstr_username, mstr_password, project_id=project_id, login_mode=login_mode)
conn.connect()

# if command line parameter = reset then reset cube
if sys.argv[1] == 'reset': 

   # create dataset and write dataset_id to dat file 
   ds = Dataset(connection=conn, name="system_monitor_cube")
   ds.add_table(name="readings", data_frame=df_system_cpu, update_policy="Add")
   ds.create(folder_id="E59B6FE611EA21D4EA960080EFC58828")
   print("Cube reset is instantiated... New dataset ID: " + ds.dataset_id)

   # open dat file to write dataset_id
   file = open('dataset_id.dat', 'w')
   file.write(ds.dataset_id + '\n')
   file.close()

else:

   print("Cube reset not instantiated...")

#read dataset_id from dat file
file = open('dataset_id.dat', 'r')
file_dataset_id=file.read().replace("\n", "")
print("Using dataset ID: " + file_dataset_id)
file.close()

#connection to MSTR dataset
ds = Dataset(connection=conn, dataset_id=file_dataset_id)

#kafka connection
consumer = KafkaConsumer(
    'system_monitor',
     bootstrap_servers=['192.168.56.101:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

#counter
counter = 0

#read msg, append to df; every 10th msg write DF to MSTR and flush
for message in consumer:

    counter += 1
    message = message.value
    df_system_cpu = df_system_cpu.append(message, ignore_index=True)
    print(message)

    if counter % 10 == 0:

        print(df_system_cpu.tail(5).to_string())
        print('writing to cube...')

        ds.add_table(name="readings", data_frame=df_system_cpu, update_policy="add")
        ds.update()
        ds.publish()


