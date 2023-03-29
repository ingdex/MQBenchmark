#!/bin/bash

#export testTarget="RocketMQ"
export RocketMQPath="/root/rocketmq-4.9.4"
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

restartRocketMQ