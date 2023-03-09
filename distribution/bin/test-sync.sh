#!/bin/bash

result-filename=result-sync.txt

doTest(){
  echo "\n$1\n" >> $result-filename
  # 执行程序A，并将其输出重定向到文件中
  ./rmqproducer.sh -c ../conf/$1 > output.log &
  sleep 5m
  # RMQProducerPerf
  PID=$(ps -ef | grep "RMQProducerPerf" | grep -v grep | awk '{print $2}')
  kill -9 $PID
  sleep 1m
  # 获取输出文件中第2到11行以"Current Time: "开头的内容，并将结果打印出来
  grep -o '^Current Time: .*' output.log | sed -n '2,11p' >> $result-filename
}

# 外层循环遍历数字i，i的取值为8、16、32、64
for i in 8 16 32 64
do
  # 内层循环遍历数字j，j的取值为1024、4096、8192、16384、32768、65536、131072、1048576
  for j in 1024 4096 8192 16384 32768 65536 131072 1048576
  do
    filename="rmq$i-1-$j-sync.json"
    doTest $filename
  done
done