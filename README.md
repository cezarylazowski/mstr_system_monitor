## System monitoring using python, push data to MSTR cube 

## install software & packages
sudo yum install gcc <br />
sudo yum install python3-devel OR sudo yum install python3-dev <br />
sudo pip3 install psutil <br /> 
pip3 install kafka-python <br />
pip3 install kafka <br />

## start zookeeper & kafka server in the background
cd /opt/kafka_2.12-2.3.0 <br /> 
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties  <br />
bin/kafka-server-start.sh -daemon config/server.properties <br />

## create/list/describe topic
cd /opt/kafka_2.12-2.3.0  <br />
bin/kafka-topics.sh --create --zookeeper 192.168.56.101:2181 --replication-factor 1 --partitions 1 --topic system_monitor <br />
bin/kafka-topics.sh --list --zookeeper 192.168.56.101:2181 <br />
bin/kafka-topics.sh --describe --zookeeper 192.168.56.101:2181 --topic system_monitor <br />

## run python producer
python3 producer.py  <br />

## run python consumer
python3 consumer.py reset OR <br />
python3 consumer.py noreset <br />

## cleanup
cd /opt/kafka_2.12-2.3.0  <br />
bin/kafka-topics.sh --delete --zookeeper 192.168.56.101:2181 --topic system_monitor <br />
bin/kafka-topics.sh --create --zookeeper 192.168.56.101:2181 --replication-factor 1 --partitions 1 --topic system_monitor <br />
bin/kafka-topics.sh --list --zookeeper 192.168.56.101:2181 <br />
