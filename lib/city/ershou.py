#!/usr/bin/env python
# coding=utf-8
# author: zengyuetian
# 二手房信息的数据结构

import sys
from lib.utility.version import PYTHON_3
if not PYTHON_3:   # 如果小于Python3
    reload(sys)
    sys.setdefaultencoding("utf-8")


# class ErShou(object):
#     def __init__(self, district, area, name, price, desc):
#         self.district = district
#         self.area = area
#         self.price = price
#         self.name = name
#         self.desc = desc
#
#     def text(self):
#         return self.district + "," + \
#                 self.area + "," + \
#                 self.name + "," + \
#                 self.price + "," + \
#                 self.desc
class ErShou(object):
    def __init__(self, district, area, desc, position, price, price_unit):
        self.district = district
        self.area = area
        self.desc = desc
        self.position = position
        self.price = price
        self.price_unit = price_unit

    def text(self):
        return self.district + "," + \
                self.area + "," + \
                self.desc + "," + \
                self.position + "," + \
                self.price + "," + \
                self.price_unit
