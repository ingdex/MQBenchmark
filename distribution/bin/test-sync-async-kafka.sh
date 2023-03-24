#!/bin/bash

export testTarget="async-kafka"
export KafkaPath="/root/kafka_2.13-3.3.1-modify"
export KafkaLogDir="/root/data"
export KafkaProcessName="Kafka"
export ZookeeperProcessName="QuorumPeerMain"

restartKafka() {
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
  cd $KafkaPath
  echo $(date) >> nohup.out
  nohup sh ./bin/zookeeper-server-start.sh config/zookeeper.properties &
  sleep 5s
  nohup sh ./bin/kafka-server-start.sh config/server.properties > broker.nohup1 2> broker.nohup2 &
  sleep 10s
}

doTest() {
  echo "\n$1\n" >> $2
  # 执行程序A，并将其输出重定向到文件中
  ./kafkaproducer.sh -c ../conf/$1 > output.log &
  sleep 3m
  # RMQProducerPerf
  PID=$(ps -ef | grep "KafkaProducerPerf" | grep -v grep | awk '{print $2}')
  kill -9 $PID
  sleep 5s
  grep -o '^Current Time: .*' output.log >> $2
}

export path=$(pwd)

if jps | grep -v grep | grep $KafkaProcessName >/dev/null; then
  echo "$KafkaProcessName进程存在"
else
  echo "$KafkaProcessName进程不存在，启动B$KafkaProcessName"
  restartKafka
fi

# async
# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64 128 256; do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048576; do
    USAGE=$(df -h $KafkaLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 60 ]; then
      echo "已用存储空间大于60%！重启"
      restartKafka
    fi
    configFilename="kafka-$i-1-$j.json"
    resultFilename="result-$testTarget-async-producer.txt"
    cd $path
    doTest $configFilename $resultFilename
  done
done

#restartKafka