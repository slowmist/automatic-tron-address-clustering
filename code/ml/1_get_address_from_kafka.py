import redis
from kafka import KafkaConsumer
from tronpy import Tron
from tronpy.providers import HTTPProvider
import requests
import time
import json

tronprovider = HTTPProvider(endpoint_uri="http://127.0.0.1:8090", timeout=3000)
client = Tron(provider=tronprovider)
pool = redis.ConnectionPool(host='localhost', port=6379,
                            max_connections=10000, db=0, decode_responses=True)
redis_conn = redis.Redis(connection_pool=pool)

# raw data like below
'''
ConsumerRecord(topic = 'solidityevent', partition = 0, offset = 522530596, timestamp = 1665738387896, timestamp_type = 0, key = None, value = {
'timeStamp': 1665738333000,
'triggerName': 'solidityEventTrigger',
'uniqueId': '8604df83e37e1b53bd0a3d31ad118f9ec8119dbc2444e42e8cb5a9bbf36cbd12_1',
'transactionId': '8604df83e37e1b53bd0a3d31ad118f9ec8119dbc2444e42e8cb5a9bbf36cbd12',
'contractAddress': 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t',
'callerAddress': '',
'originAddress': 'TV6MuMXfmLbBqPZvBHdwFsDnQeVfnmiuSi',
'creatorAddress': 'THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC',
'blockNumber': 45052580,
'blockHash': '0000000002af72a46e08181c0990f0af6e839ae020795f13d3faae7c908954b4',
'removed': False,
'latestSolidifiedBlockNumber': 45052561,
'logInfo': None,
'rawData': {
'address': 'a614f803b6fd780986a42c78ec9c7f77e6ded13c',
'topics': ['ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '000000000000000000000000d1c4bb7b2f39aba5707711719b2236b5b605af2e', '000000000000000000000000caf2ad6b6afd5d10484461057b9fe18c3c070ddc'],
'data': '0000000000000000000000000000000000000000000000000000000007c17940'
},
'abi': None,
'eventSignature': 'Transfer(address,address,uint256)',
'eventSignatureFull': 'Transfer(address from,address to,uint256 value)',
'eventName': 'Transfer',
'topicMap': {
'0': 'TV6MuMXfmLbBqPZvBHdwFsDnQeVfnmiuSi',
'1': 'TUUJDLBTGre9Hc8mgMcAA9hXBFP63d8Lwz',
'from': 'TV6MuMXfmLbBqPZvBHdwFsDnQeVfnmiuSi',
'to': 'TUUJDLBTGre9Hc8mgMcAA9hXBFP63d8Lwz'
},
'dataMap': {
'2': '130120000',
'value': '130120000'
}
}, headers = [], checksum = 1724271152, serialized_key_size = -1, serialized_value_size = 1318, serialized_header_size = -1)
'''


# Get transaction type according to 4tye data, you can refer to https://www.4byte.directory/
def get_info(tx_id: str):
    function_name = ""
    _4byte = ""
    while True:
        try:
            data = client.get_transaction(tx_id)
            _4byte = '0x' + \
                     data['raw_data']['contract'][0]['parameter']['value']['data'][:8]
            # file download from https://www.4byte.directory/
            function_name = json.load(open('./_4byte_dic.json'))[_4byte]
            return function_name
        except requests.exceptions.ProxyError:
            time.sleep(3)
            continue
        except Exception as e:
            with open("./tron_error.log", "a") as f:
                f.writelines(tx_id + "\n")
            return function_name


def save_data(it, precision):
    transaction_hash = timestamp = from_address = to_address = value = function_name = ""
    transaction_hash = it['transactionId']
    timestamp = it['timeStamp']
    from_address = it['topicMap']['from']
    to_address = it['topicMap']['to']
    value = int(it['dataMap']['value']) / (10 ** precision)
    # get transaction type
    function_name = get_info(transaction_hash)
    try:
        with open('./transactions.csv', 'a') as f:
            f.writelines(transaction_hash + ',' + str(timestamp) + ',' + from_address + ',' + to_address + ',' + str(
                value) + ',' + function_name + '\n')
        return True
    except Exception as e:
        return False


if __name__ == "__main__":
    consumer = KafkaConsumer('solidityevent', auto_offset_reset='earliest', group_id='tron_redis', bootstrap_servers=[
        'localhost:9092'], value_deserializer=lambda m: json.loads(m.decode('ascii')))

    for msg in consumer:
        item = msg[6]
        contract_address = item['contractAddress']
        # get USDT Token
        if contract_address == 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t':
            redis_conn.set(item['topicMap']['from'], hash(item['topicMap']['from']))
            redis_conn.set(item['topicMap']['to'], hash(item['topicMap']['to']))
            save_data(item, 6)
        # get USDC Token
        elif contract_address == "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8":
            redis_conn.set(item['topicMap']['from'], hash(item['topicMap']['from']))
            redis_conn.set(item['topicMap']['to'], hash(item['topicMap']['to']))
            save_data(item, 6)
