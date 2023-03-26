#!/bin/bash

export testTarget="kafka"
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

doTest() {
  echo "\n$1\n" >>$2
  # 执行程序A，并将其输出重定向到文件中
  ./kafkaproducer.sh -c ../conf/$1 >output.log &
  sleep 4m
  # RMQProducerPerf
  PID=$(ps -ef | grep "KafkaProducerPerf" | grep -v grep | awk '{print $2}')
  kill -9 $PID
  sleep 5s
  grep -o '^Current Time: .*' output.log >>$2
}

export path=$(pwd)

# async

cp /root/kafka_2.13-3.3.1-modify/config/asyncserver.properties /root/kafka_2.13-3.3.1-modify/config/server.properties
restartKafka
# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64 128 256; do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048488; do
    USAGE=$(df -h $KafkaLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 60 ]; then
      echo "已用存储空间大于60%！重启"
      restartKafka
    fi
    configFilename="kafka-$i-1-$j.json"
    resultFilename="result-async-$testTarget-async-producer.txt"
    cd $path
    doTest $configFilename $resultFilename
  done
done

# sync

cp /root/kafka_2.13-3.3.1-modify/config/syncserver.properties /root/kafka_2.13-3.3.1-modify/config/server.properties
restartKafka
# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64 128 256; do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048488; do
    USAGE=$(df -h $KafkaLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 60 ]; then
      echo "已用存储空间大于60%！重启"
      restartKafka
    fi
    configFilename="kafka-$i-1-$j.json"
    resultFilename="result-sync-$testTarget-async-producer.txt"
    cd $path
    doTest $configFilename $resultFilename
  done
done

# async

cp /root/kafka_2.13-3.3.1-modify/config/async1000server.properties /root/kafka_2.13-3.3.1-modify/config/server.properties
restartKafka
# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64 128 256; do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048488; do
    USAGE=$(df -h $KafkaLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 60 ]; then
      echo "已用存储空间大于60%！重启"
      restartKafka
    fi
    configFilename="kafka-$i-1-$j.json"
    resultFilename="result-async1000-$testTarget-async-producer.txt"
    cd $path
    doTest $configFilename $resultFilename
  done
done
