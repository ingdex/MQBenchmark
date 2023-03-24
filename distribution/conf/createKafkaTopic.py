# 设置topic的数量
num_topics = 128

# 生成JSON数据
data = []
for i in range(num_topics):
    cmd = "./kafka-topics.sh --create --topic topic{} --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092".format(i)
    data.append(cmd)

# 将JSON数据输出到文件中
with open("createKafkaTopic.sh", mode="w") as f:
    for elem in data:
        f.write(str(elem) + "\n")
