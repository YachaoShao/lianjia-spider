#-*- coding:utf8 -*-

#/home/yachao/github/lianjia-spider/data/ershou/bj/20190325
#coding=utf-8
import os
import pandas as pd
import glob
import csv

def merge(file_path):
    files = os.listdir(file_path)
    print(u'共发现%s个CSV文件'% len(files))
    print(u'正在处理............')
    fout = open("second_hand_price_beijing_with_position.csv","wb")
    for i in files:
        # if os.stat(i).st_size == 0:
        #     pass
        # else:
            # fr = open(i,'r').read()
            print('processing file：'+i)
            file_path = path +'/'+i
            file_input = open(file_path,'rb')
           # reader = csv.reader(file_input)
           # for line in reader:
            for line in file_input:
                fout.write(line)
            #with open('second_hand_price_beijing.csv','a') as f:
               # f.write(fr)
            fout.close
    print(u'合并完毕！')

def delete_duplicates(merged_file):
    reader = pd.read_csv(merged_file)
    datalist = reader.drop_duplicates()
    datalist.to_csv(merged_file)

if __name__ == '__main__':
    path = '/home/yachao/github/lianjia-spider/data/ershou/bj/20190326'
    merge(path)
    delete_duplicates('second_hand_price_beijing.csv')
        
