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

producer = KafkaProducer(bootstrap_servers=['192.168.56.101:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

for e in range(10000):

    #set up variables
    timestamp = datetime.now().isoformat()
    cpu = psutil.cpu_percent(interval=None, percpu=False)
    mem_avail = psutil.virtual_memory().available
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)

    #create msg
    msg = {'hostname' : hostname, 'id_address' : ip_address, 'timestamp' : timestamp, 'cpu' : cpu, 'mem' : mem_avail}
    print(msg)

    #publish msg
    producer.send('system_monitor', value=msg)

    sleep(3)