# !/usr/bin/env
# -*- coding:utf-8 -*-
# Yachao Shao
# Code on 4th April 2019


import pandas as pd
import  time


def clean_raw_data(file_name):
    datas = []
    raw_data = pd.read_csv(file_name, names=['date','strict','xiaoqu','more_details','position','price','unit_price'])
    clean_data = pd.DataFrame(columns=['strict','xiaoqu','building','sleeping_room','drawing_room','building_area','direction',
                                       'house_design','lift','position','floors_building','building_year','price','unit_price','full_address'])
    clean_data['strict'] = raw_data['strict']
    clean_data['xiaoqu'] = raw_data['xiaoqu']
    clean_data['price'] = raw_data['price'].str[:-1].astype(float)
    clean_data['unit_price'] = raw_data['unit_price'].str[2:-4].astype(float)
    basic_information = '北京' + raw_data['strict'] +raw_data['xiaoqu'] + raw_data['more_details']
    basic_information = basic_information.str.split('/', expand=True)
    clean_data['full_address'] = basic_information[0]
    clean_data['sleeping_room'] = basic_information[1].str[0].astype(int)
    clean_data['drawing_room'] = basic_information[1].str[2].astype(int)
    clean_data['building_area'] = basic_information[2].str[:-2].astype(float)
    clean_data['direction'] = basic_information[3]
    clean_data['house_design'] = basic_information[4]
    clean_data['lift'] = basic_information[5]
    position_infomation = raw_data['position'].str.split('/', expand=True)
    clean_data['position'] = position_infomation[0].str[0]
    clean_data['floors_building'] = position_infomation[0]
    clean_data['building_year'] = position_infomation[1]
    clean_data.to_csv('cleaned_data_beijing_20190401.csv', index=False)


if __name__ == '__main__':
    file_path = '/home/yachao/github/lianjia-spider/ershou_merged_20190401.csv'
    start_time = time.time()
    print("start cleaning raw data ")
    # file_path = '/Users/shaoyc/Github/lianjia-spider/merged_example.csv'
    print("start cleaning raw data %s" %file_path +"......")
    clean_raw_data(file_path)
    end_time = time.time()
    print("Finish cleaning, Using %f" % (end_time - start_time))

