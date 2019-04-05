# !/usr/bin/env
# -*- coding:utf-8 -*-
# Yachao Shao
# Code on 5th April 2019


AK_list = []
ak_i = 0

fin = open('AK.txt', 'r')
while (1):
    line = fin.readline().strip()
    if line == '':
        break
    AK_list.append(line)

fin.close()

print(AK_list)