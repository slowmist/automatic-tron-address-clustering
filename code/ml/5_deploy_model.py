import redis
from kafka import KafkaConsumer
import json
import pymysql
from datetime import datetime
from dateutil.parser import parse
import xgboost as xgb


pool = redis.ConnectionPool(host='localhost', port=6379,
                            max_connections=10000, db=1, decode_responses=True)
redis_conn = redis.Redis(connection_pool=pool)

db = pymysql.connect(host='localhost',
                     user='root',
                     password='123456',
                     database='label')


def save_to_mysql(address: str, tag: str):
    cursor = db.cursor()
    try:
        result = cursor.execute('select * from info where address="%s";' % address)
        data = result.fetchone()
        if data:
            cursor.execute('UPDATE info SET tag="%s" WHERE address ="%s";' % tag, address)
            db.commit()
        else:
            cursor.execute('insert into info (address, tag) values ("%s", "%s");' % address, tag)
            db.commit()
    except Exception as e:
        print(e)
    cursor.close()


def predict(columns):
    model_xgb = xgb.Booster()
    model_xgb.load_model("./boost.model")
    r = model_xgb.predict(columns)
    return r


# ['out_count', 'in_count', 'out_addr_count', 'in_addr_count', 'out_avg', 'in_avg', 'gap_time']
def calculate_feature(addr: str, data):
    if redis_conn.exists(addr):
        record = redis_conn.lrange(addr, 1, 7)
        if data[0] == 'to':
            date1 = datetime.fromtimestamp(record[6])
            date2 = datetime.fromtimestamp(data[3])
            gap = (parse(str(date1)) - parse(str(date2))).total_seconds()//60
            features = [record[0]+1, record[1], record[2]+1, record[3], record[4]+data[2]/record[0]+1, record[5], gap]
        else:
            date1 = datetime.fromtimestamp(record[6])
            date2 = datetime.fromtimestamp(data[3])
            gap = (parse(str(date1)) - parse(str(date2))).total_seconds()//60
            features = [record[0], record[1]+1, record[2], record[3]+1, record[4], record[5]+data[2]/record[1]+1, gap]
        redis_conn.lpush(addr, features)
    else:
        if data[0] == 'to':
            redis_conn.lpush(addr, [0, 1, 0, 1, 0, data[2], 0])
        if data[0] == 'from':
            redis_conn.lpush(addr, [1, 0, 1, 0, data[2], 0, 0])
    return redis_conn.lrange(addr, 1, 7)


if __name__ == "__main__":
    consumer = KafkaConsumer('solidityevent', auto_offset_reset='earliest', group_id='ml', bootstrap_servers=[
        'localhost:9092'], value_deserializer=lambda m: json.loads(m.decode('ascii')))

    for msg in consumer:
        item = msg[6]
        contract_address = item['contractAddress']
        
        if contract_address in ['TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 'TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8']:
            fromaddr = item['topicMap']['from']
            toaddr = item['topicMap']['to']
            value = int(item['dataMap']['value']) / 10**8
            t = item['timeStamp'] / 1000

            col = calculate_feature(fromaddr, ['to', toaddr, value, t])
            re = predict(col)
            save_to_mysql(fromaddr, re)

            col = calculate_feature(toaddr, ['from', fromaddr, value, t])
            re = predict(col)
            save_to_mysql(toaddr, re)
