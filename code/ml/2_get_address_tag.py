import redis
import requests


'''
Get address tag from https://tronscan.org if exist.
'''
base_url = "https://apilist.tronscan.org/api/account?address="

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

data = r.keys()

for addr in data:
    url = base_url + addr.strip()
    print(url)
    try:
        result = requests.get(url, timeout=8).json()
        if 'addressTag' in result.keys():
            tag = result['addressTag']
            with open("./data.csv", "a") as f:
                f.writelines(addr + "," + tag + "\n")
    except requests.exceptions.RequestException as e:
        print(e)
        with open("./error.txt", "a") as k:
            k.writelines(addr + "\n")