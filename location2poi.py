# !/usr/bin/env
# -*- coding:utf-8 -*-
# Yachao Shao
# Code on 5th April 2019
# This file is used to get POI information from our transfered location list

import json
import urllib
import math
import pandas as pd
import time



def gcj02_to_bd09(lng, lat):

    x_pi = 3.14159265358979324 * 3000.0 / 180.0
    pi = 3.1415926535897932384626  # π
    a = 6378245.0  # 长半轴
    ee = 0.00669342162296594323  # 偏心率平方
    """
    火星坐标系(GCJ-02)转百度坐标系(BD-09)
    谷歌、高德——>百度
    :param lng:火星坐标经度
    :param lat:火星坐标纬度
    :return:
    """
    z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * x_pi)
    theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * x_pi)
    bd_lng = z * math.cos(theta) + 0.0065
    bd_lat = z * math.sin(theta) + 0.006
    return [bd_lng, bd_lat]


def location_poi(baidu_location, baidu_api_key):

    tag_list = ['美食', '酒店', '购物', '生活服务', '丽人', '旅游景点','休闲娱乐','运动健身',
         '教育培训', '文化传媒', '医疗', '汽车服务','交通设施', '金融','房地产', '公司企业',
         '政府机构','出入口', '自然地物']

    locationx = str(baidu_location[1])
    locationy = str(baidu_location[0])

    url1 = 'http://api.map.baidu.com/place/v2/search?query='
    url2 = '&location='
    url3 = '&radius=500&output=json&scope=2&page_size=20&page_num=0&sort_name:distance|sort_rule:1&ak='
    'fBUwUjXAwBh1jxTLRdhubR0bG2buPCFD'
    poi_info_list = []
    poi_info_list.append(baidu_location)
    tag = 0
    while tag < len(tag_list):
        # print(tag_list[tag])
        url = url1 + urllib.parse.quote(tag_list[tag]) + url2 + locationx + ',' + locationy + url3 + baidu_api_key
        # print(url)
        result = urllib.request.urlopen(url).read()
        poi = json.loads(result)
        # print(poi)

        if poi['message'] == 'ok':
            number_total = poi['total']
            results = poi['results']
            distance_ave = 0
            if number_total != 0:
                distance_sum = 0
                for i in range(len(results)):
                    distance_sum = distance_sum + results[i]['detail_info']['distance']
                distance_ave = distance_sum/number_total
            poi_info_list.append(number_total)
            poi_info_list.append(distance_ave)
            tag += 1
    # print(poi_info_list)
    return poi_info_list
    # input("Press Enter to continue...")


if __name__ == '__main__':
    time_start = time.time()
    print("start extract poi information from baidu......")
    # creat baidu_APIKEY list
    AK_list = []
    fin = open('AK.txt', 'r')
    while (1):
        line = fin.readline().strip()
        if line == '':
            break
        AK_list.append(line)

    fin.close()


    # location_file = '/Users/shaoyc/PycharmProjects/lianjia-spider/address_location_detail.csv'
    location_file = 'address_location_detail_ershou_20190401.csv'
    location_list = pd.read_csv(location_file)['location']
    columns_name = ['baidu_location', 'poi_1_number', 'poi_1_distance_ave',
               'poi_2_number', 'poi_2_distance_ave', 'poi_3_number', 'poi_3_distance_ave',
               'poi_4_number', 'poi_4_distance_ave', 'poi_5_number', 'poi_5_distance_ave',
               'poi_6_number', 'poi_6_distance_ave', 'poi_7_number', 'poi_7_distance_ave',
               'poi_8_number', 'poi_8_distance_ave', 'poi_9_number', 'poi_9_distance_ave',
               'poi_10_number', 'poi_10_distance_ave', 'poi_11_number', 'poi_11_distance_ave',
               'poi_12_number', 'poi_12_distance_ave', 'poi_13_number', 'poi_13_distance_ave',
               'poi_14_number', 'poi_14_distance_ave', 'poi_15_number', 'poi_15_distance_ave',
               'poi_16_number', 'poi_16_distance_ave', 'poi_17_number', 'poi_17_distance_ave',
               'poi_18_number', 'poi_18_distance_ave', 'poi_19_number', 'poi_19_distance_ave']
    location_poi_list = pd.DataFrame(columns=columns_name)

    i = 0
    for location in location_list:
        lng, lat = location.split(',')
        lng = float(lng)
        lat = float(lat)
        baidu_location = gcj02_to_bd09(lng, lat)
        location_poi_list.loc[len(location_poi_list)]=location_poi(baidu_location, AK_list[i//2000 + 1])
        i = i + 1
        print("Finish transfer %d location's POI extracting."%i)
    location_poi_list['location'] = location_list
    location_poi_list.to_csv("location_poi_beijing_ershou_0401.csv")
    time_end = time.time()
    print("Finished all POI information extration, using %f."%(time_end-time_start))






