# !/usr/bin/env
# -*- coding:utf-8 -*-
'''
利用高德地图api实现经纬度与地址的批量转换
'''
import requests
import pandas as pd
import time
import http.client
import json
import urllib

base = '/v3/geocode/geo'


def parse(file_name):
    datas = []
    raw_data = pd.read_csv(file_name, names=['date','strict','xiaoqu','more_details','position','price','unit_price'])
    raw_data_dict = raw_data.to_dict('index')
    for i in range(0, len(raw_data_dict)):
        address_detail = str(raw_data_dict[i]['strict']) + str(raw_data_dict[i]['xiaoqu']) + str(raw_data_dict[i]['more_details'])
        address_detail = address_detail.split('/')[0]
        datas.append(address_detail)
    datas = list(dict.fromkeys(datas))
    with open('address_list', 'w') as address_list_txt:
        for item in datas:
            address_list_txt.write("%s\n" % item)
    return datas


# def transform(location):
#     parameters = {'coordsys': 'gps', 'locations': location, 'key': key_gaode}
#     base = 'http://restapi.amap.com/v3/assistant/coordinate/convert'
#     response = requests.get(base, parameters)
#     answer = response.json()
#     return answer['locations']

# address to location(latitude,longitude)
# def geocode(address, key_gaode):
#     parameters = {'location': address, 'key': key_gaode}
#     base = 'http://restapi.amap.com/v3/geocode/regeo'
#     response = requests.get(base, parameters)
#     answer = response.json()
#     print(address + "的经纬度：", answer['geocodes'][0]['location'])
def geocode(address, key):

    path = '{}?address={}&key={}'.format(base, urllib.parse.quote(address), key)
    connection = http.client.HTTPConnection('restapi.amap.com', 80)
    connection.request('GET', path)
    rawreply = connection.getresponse().read()
    reply = json.loads(rawreply.decode('utf-8'))
    location = reply['geocodes'][0]['location']
    return location


if __name__ == '__main__':
    print("start to transfer address to location......")
    time_start = time.time()
    # locations = parse(item)
    file_path = '/home/yachao/github/lianjia-spider/ershou_merged_20190401.csv'
    address_list = parse(file_path)
    print(len(address_list))
    i = 0
    df = pd.DataFrame(columns=['address', 'location'])
    key_test = ['a73877a9bbf91ae2f90b80c96a60b2b5','3128897cfac897df4fd250d471d412ba','4cf1979dd1c596e80751fabe4686c3f1','8cba5174d15beb23c23f4dd0b3d6489c']
    for addr in address_list:
        addr = '北京' + addr
        print(addr)
        location = geocode(addr, key_test[i/3000])
        df.loc[len(df)] = [addr, location]
        i = i + 1
        print(i)
    df.to_csv('address_location_detail_ershou_20190401.csv', index=False)
    time_end = time.time()
    print("Tranfering finished, using %f" % (time_end - time_start))
