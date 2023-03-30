#!/bin/bash

#msgSize=(1024 4096 8192 16384 32768 65536 131072 1048576)
rootPath=$(pwd)
filename="$rootPath/result-kafka-consumer.txt"

export KafkaPath="/root/kafka_2.13-3.3.1-modify"
export KafkaLogDir="/root/data"
export KafkaProcessName="Kafka"
export ZookeeperProcessName="QuorumPeerMain"
export ZookeeperPort=2181
export KafkaPort=9092

shutdownKafka0() {
  PID=$(jps | grep $KafkaProcessName | grep -v grep | awk '{print $1}')
  kill -9 $PID
  sleep 10s
  PID=$(jps | grep $ZookeeperProcessName | grep -v grep | awk '{print $1}')
  kill -9 $PID
  sleep 10s
  rm -r $KafkaLogDir/kafka-gc-logs
  sleep 1s
  rm -r $KafkaLogDir/kafka-logs
  sleep 1s
  rm -r $KafkaLogDir/zookeeper
  sleep 1s
}

shutdownKafka() {
  shutdownKafka0
  # 需要检查的端口号
  # 检查指定端口是否被占用
  while true; do
    kafkaStatus=$(lsof -i:$KafkaPort | grep LISTEN)
    zookeeperStatus=$(lsof -i:$ZookeeperPort | grep LISTEN)
    # 如果端口被占用
    if [ -z "$kafkaStatus" ] && [ -z "$zookeeperStatus" ]; then
      break
    fi
    shutdownKafka0
  done
}

restartKafka() {
  shutdownKafka
  cd $KafkaPath
  nohup sh ./bin/zookeeper-server-start.sh config/zookeeper.properties &
  sleep 5s
  nohup sh ./bin/kafka-server-start.sh config/server.properties >broker.nohup1 2>broker.nohup2 &
  sleep 10s
}

# param 1: message size
produceEnoughMsg() {
  cd $rootPath
  ./kafkaproducer.sh -c ../conf/kafka-16-1-$1.json &
  while true; do
    USAGE=$(df -h $KafkaLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 60 ]; then
      echo "已用存储空间大于60%"
      # RMQProducerPerf
      PID=$(ps -ef | grep "KafkaProducerPerf" | grep -v grep | awk '{print $2}')
      kill -9 $PID
      sleep 2s
      break
    else
      sleep 200
    fi
  done
}

for size in 1024 4096 8192 16384 32768 65536 131072 1048576;
do
  restartKafka
  echo -e "\n$size\n" >> $filename
  produceEnoughMsg $size
  cd $rootPath
  ./kafkaconsumer.sh -c ../conf/kafkaConsumer-16-1.json > output.log &
  sleep 2m
  # kill RMQConsumerPerf
  PID=$(ps -ef | grep "KafkaConsumerPerf" | grep -v grep | awk '{print $2}')
  kill -9 $PID
  sleep 2s
  # 获取输出文件中第2到11行以"Current Time: "开头的内容，并将结果打印出来
  grep -o '^Current Time: .*' output.log >> $filename
done
