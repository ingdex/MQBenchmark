import json

# 设置topic的数量
numTopics = [8, 16, 32, 64, 128, 256]
messageSize = [1024, 4096, 8192, 16384, 32768, 65536, 131072, 1048488]
asyncEnable = [True, False]

def do_gen_conf(num_topics, message_size, async_enable):
    # 生成JSON数据
    json_data = []
    filename = "kafka-{}-1-{}".format(num_topics, message_size)
    if not async_enable:
        filename = filename + "-sync.json"
    else:
        filename = filename + ".json"

    for i in range(num_topics):
        topic = "topic{}".format(i)
        data = {
            "topic": topic,
            "topicNum": 1,
            "messageNum": 0,
            "messageSize": message_size,
            "producerProps": ["bootstrap.servers=192.168.0.200:9092"],
            "threadNum": 1,
            "asyncEnable": async_enable
        }
        # data = {
        #     "topic": topic,
        #     "topicCount": 1,
        #     "messageSize": message_size,
        #     "keyEnable": False,
        #     "propertySize": 0,
        #     "tagCount": 0,
        #     "msgTraceEnable": False,
        #     "aclEnable": False,
        #     "messageNum": 0,
        #     "delayEnable": False,
        #     "delayLevel": 1,
        #     "asyncEnable": async_enable,
        #     "threadNum": 1,
        #     "nameServer": "192.168.0.181:9876"
        # }
        json_data.append(data)

    # 将JSON数据输出到文件中
    with open(filename, "w") as f:
        json.dump(json_data, f, indent=2)

# numTopics = [8, 16, 32, 64, 128, 256]
# messageSize = [1024, 4096, 8192, 16384, 32768, 65536, 131072, 1048488]
# asyncEnable = [True, False]
for n in numTopics:
    for s in messageSize:
        for sync in asyncEnable:
            do_gen_conf(n, s, sync)