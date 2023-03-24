import json

# 设置topic的数量
num_topics = 128

# 生成JSON数据
json_data = []
for i in range(num_topics):
    topic = "topic{}".format(i)
    data = {
        "topic": topic,
        "topicNum": 1,
        "messageNum": 0,
        "messageSize": 1024,
        "producerProps": ["bootstrap.servers=192.168.0.200:9092"],
        "threadNum": 1,
        "asyncEnable": True
    }
    json_data.append(data)

# 将JSON数据输出到文件中
with open("data.json", "w") as f:
    json.dump(json_data, f, indent=2)
