#!/bin/bash

export testTarget="sync-rocketmq"
export testMyRocketMQ=0

export originRocketMQPath="/root/rocketmq-4.9.4/"
export myRocketMQPath="/root/rocketmq/distribution/target/rocketmq-4.9.4/rocketmq-4.9.4/"
export rocketMQLogDir="/root/data"
export rocketMQProcessName="BrokerStartup"

restartRocketMQ() {
  PID=$(jps | grep $rocketMQProcessName | grep -v grep | awk '{print $1}')
  kill -9 $PID
  sleep 10s
  rm -r $rocketMQLogDir/rocketmq
  sleep 10s
  cd "$1/bin"
  echo $(date) >> nohup.out
  nohup sh mqbroker -c $1/conf/broker.conf -n 192.168.0.181:9876 >> nohup.out &
  sleep 30s
}

doTest() {
  echo "\n$1\n" >>$2
  # 执行程序A，并将其输出重定向到文件中
  ./rmqproducer.sh -c ../conf/$1 > output.log &
  sleep 4m
  # RMQProducerPerf
  PID=$(ps -ef | grep "RMQProducerPerf" | grep -v grep | awk '{print $2}')
  kill -9 $PID
  sleep 20s
  # 获取输出文件中第2到11行以"Current Time: "开头的内容，并将结果打印出来
  grep -o '^Current Time: .*' output.log >> $2
}

export path=$(pwd)

if jps | grep -v grep | grep $rocketMQProcessName >/dev/null; then
  echo "BrokerStartup进程存在"
else
  echo "BrokerStartup进程不存在，启动BrokerStartup"
  if [ $testMyRocketMQ -eq 1 ]; then
    restartRocketMQ $myRocketMQPath
  else
    restartRocketMQ $originRocketMQPath
  fi
fi

# sync
# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64; do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048576; do
    USAGE=$(df -h $rocketMQLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 50 ]; then
      echo "已用存储空间大于50%！重启BrokerStartup"
      if [ $testMyRocketMQ -eq 1 ]; then
        restartRocketMQ $myRocketMQPath
      else
        restartRocketMQ $originRocketMQPath
      fi
    fi
    configFilename="rmq$i-1-$j-sync.json"
    resultFilename="result-$testTarget-sync-producer.txt"
    cd $path
    doTest $configFilename $resultFilename
  done
done

# async
# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64; do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048576; do
    USAGE=$(df -h $rocketMQLogDir | awk '{print $5}' | tail -n 1 | sed 's/%//')
    if [ $USAGE -gt 50 ]; then
      echo "已用存储空间大于50%！重启BrokerStartup"
      if [ $testMyRocketMQ -eq 1 ]; then
        restartRocketMQ $myRocketMQPath
      else
        restartRocketMQ $originRocketMQPath
      fi
    fi
    configFilename="rmq$i-1-$j.json"
    resultFilename="result-$testTarget-async-producer.txt"
    cd $path
    doTest $configFilename $resultFilename
  done
done

