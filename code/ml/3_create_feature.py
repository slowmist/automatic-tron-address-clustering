import pandas as pd
import random

df = pd.read_csv('./train.csv', names=['from_addr', 'to_addr', 'timestamp', 'value'])

# The tags are from https://tronscan.org
hot = []
cold = []

data = random.sample(df['from_addr'].to_list(), 1200)
common = (set(data) - set(cold))
common = list(common - set(hot))
common = list(common)


def min_gap(nums):
    if len(nums) < 2:
        return 0
    gap = nums[1] - nums[0]
    for i in range(2, len(nums)):
        temp = nums[i] - nums[i - 1]
        if temp < gap:
            gap = temp
    return gap


def get_feature(address: str):
    out_count = df[df['from_addr'] == address].shape[0]
    in_count = df[df['to_addr'] == address].shape[0]
    if out_count == 0:
        out_addr_count = 0
        out_avg = 0
    else:
        out_addr_count = df[df['from_addr'] == address]['to_addr'].nunique()
        out_avg = df[df['from_addr'] == address]['value'].sum() / out_count
    if in_count == 0:
        in_addr_count = 0
        in_avg = 0
    else:
        in_addr_count = df[df['to_addr'] == address]['from_addr'].nunique()
        in_avg = df[df['to_addr'] == address]['value'].sum() / in_count

    nums = df[(df['from_addr'] == address) | (df['to_addr'] == address)]['timestamp'].unique()
    nums.sort()
    gap_time = min_gap(nums) / 60
    return out_count, in_count, out_addr_count, in_addr_count, out_avg, in_avg, gap_time


if __name__ == '__main__':
    for item in common:
        result = get_feature(item)
        line = ''
        for s in result:
            line = line + str(s) + ","
        line = line + '3'
        print(line)
        with open('./clean.csv', 'a') as f:
            f.writelines(line + '\n')

    s = df['from_addr'].to_list() + df['to_addr'].to_list()
    k = (set(s) - set(hot)) - set(cold)
    k = list(k)

    for it in k:
        result = get_feature(it)
    line = ''
    for s in result:
        line = line + str(s) + ","
    line = line[:-1]
    print(line)
    print()
    with open('./unknown.csv', 'a') as f:
        f.writelines(line + '\n')

    with open('./unknown.csv', 'r') as f:
        data = f.readlines()
    for s, d in zip(k, data):
        print(s, d)
        with open('./unknown_label.csv', 'a') as l:
            l.writelines(s + ',' + d)
