target = set()

with open('./transaction.csv', 'r') as f:
    data = f.readlines()
    for index, line in enumerate(data):
        line = line.strip()
        line = line.split(',')
        with open('./usdc_data_new.csv', 'a') as t:
            target.add(line[2])
            target.add(line[3])

target = list(target)

for i, item in enumerate(target):
    with open('./nodes.csv', 'a') as f:
        f.writelines(str(i) + ',' + item + '\n')

with open('./usdc_data_new.csv', 'r') as f:
    data = f.readlines()
    for line in data:
        line = line.strip()
        line = line.split(',')
        with open('./edges.csv', 'a') as t:
            t.writelines(
                line[0] + ',' + line[1] + ',' + str(target.index(line[2])) + ',' + str(target.index(line[3])) + ',' +
                line[4] + ',' + line[5] + ',' + line[6] + ',' + line[7] + '\n')
