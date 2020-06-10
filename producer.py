#
# producer.py
# June 07, 2020
# monitor system stats like CPU/memory and publish to Kafka topic 
# command line parameters:
#     python3 producer.py reset     -- cube reset
#     python3 producer.py noreset   -- no cube reset
#

import sys
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

# connect to kafka
producer = KafkaProducer(bootstrap_servers=['192.168.56.101:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# create empty df object
df_system_cpu = pd.DataFrame(columns = ['date_time','hostname','ip_address','timestamp','cpu_count','cpu_cores','cpu_core_1','cpu_core_2','vm_avail','vm_used','swap_memory','disk_io_read','disk_io_write','net_io_sent','net_io_recv'])

df_system_cpu = df_system_cpu.astype({'date_time': 'datetime64', 'hostname': 'object','ip_address':'object','timestamp':'object','cpu_count': 'float64','cpu_cores': 'float64','cpu_core_1': 'float64','cpu_core_2': 'float64','vm_avail': 'float64','vm_used': 'float64','swap_memory': 'float64','disk_io_read': 'float64','disk_io_write': 'float64','net_io_sent': 'float64','net_io_recv': 'float64'})


# if command line parameter = reset then reset cube
if sys.argv[1] == 'reset': 

   # connect to mstr & create dataset
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
   print("Cube reset is instantiated... New dataset ID: " + ds.dataset_id)
else:
   print("Cube reset not instantiated...") 

# spool readings
for e in range(100000):

    # set up variables
    timestamp = datetime.now().isoformat()
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    
    cpu_count = psutil.cpu_count()
    cpu_cores = psutil.cpu_percent(interval=None, percpu=False)
    cpu_core_1 = psutil.cpu_percent(interval=0, percpu=True)[0]
    cpu_core_2 = psutil.cpu_percent(interval=0, percpu=True)[1]
    vm_avail = psutil.virtual_memory().available / 1024 / 1024
    vm_used = psutil.virtual_memory().used / 1024 / 1024
    swap_memory = psutil.swap_memory().free / 1024 /1024
    disk_io_read = psutil.disk_io_counters(perdisk=False).read_bytes / 1024 / 2014
    disk_io_write = psutil.disk_io_counters(perdisk=False).write_bytes / 1024 / 2014
    net_io_sent  = psutil.net_io_counters(pernic=False).bytes_sent /1024 / 1024
    net_io_recv  = psutil.net_io_counters(pernic=False).bytes_recv /1024 / 1024


    # create msg
    msg = {'date_time' : date_time, 'hostname' : hostname, 'ip_address' : ip_address, 'timestamp' : timestamp, 'cpu_count': cpu_count,'cpu_cores' :cpu_cores , 'cpu_core_1' : cpu_core_1, 'cpu_core_2': cpu_core_2,'vm_avail' :vm_avail, 'vm_used' :  vm_used, 'swap_memory' : swap_memory, 'disk_io_read' : disk_io_read, 'disk_io_write' : disk_io_write, 'net_io_sent': net_io_sent, 'net_io_recv' : net_io_recv}
 
    print(msg)

    # publish msg 
    producer.send('system_monitor', value=msg)
    
    sleep(1)