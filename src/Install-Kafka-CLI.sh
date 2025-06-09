
# Installing Kafka CLI Manually

#Enter Kafka Container
docker exec -it kafka bash

# Download Kafka Tools
wget https://downloads.apache.org/kafka/3.9.1/kafka_2.13-3.9.1.tgz
tar -xvzf kafka_2.13-3.9.1.tgz
mv kafka_2.13-3.9.1.tgz /opt/kafka

#To recheck change directory in Kafka Container
cd /tmp   # then proceed with the step below

#Set Environmental Variables
export PATH=$PATH:/opt/kafka/bin

#List Topics
kafka-topics.sh --list --bootstrap-server localhost:9092