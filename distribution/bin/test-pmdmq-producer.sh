#!/bin/bash

export testTarget="pmdmq"

export RocketMQPath="/root/rocketmq/distribution/target/rocketmq-4.9.4/rocketmq-4.9.4"
export RocketMQLogDir="/root/data"
export RocketMQProcessName="BrokerStartup"
export RocketMQPort=10911

shutdownRocketMQ0() {
  PID=$(jps | grep $RocketMQProcessName | grep -v grep | awk '{print $1}')
  kill -9 $PID
  sleep 5s
  rm -r $RocketMQLogDir/rocketmq
  sleep 1s
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

doTest() {
  echo "\n$1\n" >>$2
  # 执行程序A，并将其输出重定向到文件中
  ./rmqproducer.sh -c ../conf/$1 > output.log &
  sleep 4m
  # RMQProducerPerf
  PID=$(ps -ef | grep "RMQProducerPerf" | grep -v grep | awk '{print $2}')
  kill -9 $PID
  sleep 10s
  # 获取输出文件中第2到11行以"Current Time: "开头的内容，并将结果打印出来
  grep -o '^Current Time: .*' output.log >> $2
}

export path=$(pwd)

# sync pmdmq
testTarget="sync-pmdmq"
cp /root/broker-sync.conf $RocketMQPath/conf
restartRocketMQ

# sync producer
# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64; do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048576; do
    USAGE=$(df -h $RocketMQLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 50 ]; then
      echo "已用存储空间大于50%！重启BrokerStartup"
      restartRocketMQ
    fi
    configFilename="rmq$i-1-$j-sync.json"
    resultFilename="result-$testTarget-sync-producer.txt"
    cd $path
    doTest $configFilename $resultFilename
  done
done

# async producer
# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64; do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048576; do
    USAGE=$(df -h $RocketMQLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 50 ]; then
      echo "已用存储空间大于50%！重启BrokerStartup"
      restartRocketMQ
    fi
    configFilename="rmq$i-1-$j.json"
    resultFilename="result-$testTarget-async-producer.txt"
    cd $path
    doTest $configFilename $resultFilename
  done
done

##################
# async pmdmq
testTarget="async-pmdmq"

cp /root/broker-async.conf $RocketMQPath/conf
restartRocketMQ

# sync producer
# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64; do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048576; do
    USAGE=$(df -h $RocketMQLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 50 ]; then
      echo "已用存储空间大于50%！重启BrokerStartup"
      restartRocketMQ
    fi
    configFilename="rmq$i-1-$j-sync.json"
    resultFilename="result-$testTarget-sync-producer.txt"
    cd $path
    doTest $configFilename $resultFilename
  done
done

# async producer
# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64; do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048576; do
    USAGE=$(df -h $RocketMQLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 50 ]; then
      echo "已用存储空间大于50%！重启BrokerStartup"
      restartRocketMQ
    fi
    configFilename="rmq$i-1-$j.json"
    resultFilename="result-$testTarget-async-producer.txt"
    cd $path
    doTest $configFilename $resultFilename
  done
done
