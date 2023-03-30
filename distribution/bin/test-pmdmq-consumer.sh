#!/bin/bash

msgSize=(1024 4096 8192 16384 32768 65536 131072 1048576)
rootPath=$(pwd)
filename="$rootPath/result-pmdmq-consumer.txt"

export RocketMQPath="/root/rocketmq/distribution/target/rocketmq-4.9.4/rocketmq-4.9.4"
export RocketMQLogDir="/root/data"
export RocketMQProcessName="BrokerStartup"
export RocketMQPort=10911

shutdownRocketMQ0() {
  PID=$(jps | grep $RocketMQProcessName | grep -v grep | awk '{print $1}')
  kill -9 $PID
  sleep 5s
  rm $RocketMQLogDir/abort
  rm $RocketMQLogDir/checkpoint
  rm $RocketMQLogDir/lock
  rm -r $RocketMQLogDir/commitlog
  sleep 1s
  rm -r $RocketMQLogDir/config
  rm -r $RocketMQLogDir/consumequeue
  sleep 1s
  rm -r $RocketMQLogDir/index
  sleep 1s
  rm -r $RocketMQLogDir/rocketmqlogs
}

shutdownRocketMQ() {
  shutdownRocketMQ0
  # 需要检查的端口号
  # 检查指定端口是否被占用
  while true; do
    RocketMQStatus=$(lsof -i:$RocketMQPort | grep LISTEN)
    # 如果端口被占用
    if [ -z "$RocketMQStatus" ]; then
      break
    fi
    shutdownRocketMQ0
  done
}

restartRocketMQ() {
  shutdownRocketMQ
  cd $RocketMQPath/bin
  echo $(date) >> nohup.out
  nohup sh mqbroker -c $RocketMQPath/conf/broker.conf -n 192.168.0.181:9876 >> nohup.out &
}

# param 1: message size
produceEnoughMsg() {
  cd $rootPath
  ./rmqproducer.sh -c ../conf/rmq16-1-$1.json &
  while true; do
    USAGE=$(df -h $rocketMQLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 60 ]; then
      echo "已用存储空间大于60%"
      # RMQProducerPerf
      PID=$(ps -ef | grep "RMQProducerPerf" | grep -v grep | awk '{print $2}')
      kill -9 $PID
      sleep 2s
      break
    else
      sleep 200
    fi
  done
}

for size in ${msgSize[@]}
do
  restartRocketMQ
  echo -e "\n$size\n" >> $filename
  produceEnoughMsg $size
  cd $rootPath
  ./rmqconsumer.sh -c ../conf/rmqConsumer-16-1.json > output.log &
  sleep 2m
  # kill RMQConsumerPerf
  PID=$(ps -ef | grep "RMQConsumerPerf" | grep -v grep | awk '{print $2}')
  kill -9 $PID
  sleep 2s
  # 获取输出文件中第2到11行以"Current Time: "开头的内容，并将结果打印出来
  grep -o '^Current Time: .*' output.log >> $filename
done
