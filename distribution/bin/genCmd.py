n = 32
cmds = []
filename = "resetAllConsumeOffset.sh"
for i in range(0, n):
    data = "./mqadmin resetOffsetByTime -n 192.168.0.181:9876 -g producer_benchmark_{} -t topic{} -s -1\n".format(i, i)
    cmds.append(data)

# print(cmds)

with open(filename, "w") as f:
    for cmd in cmds:
        f.write(cmd)
