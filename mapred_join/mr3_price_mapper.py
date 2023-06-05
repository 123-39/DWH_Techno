#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_price_mapper.py"""

import sys

# Ваш код
# читаем строки из STDIN (standard input)
for line in sys.stdin:
    # Делим строку по разделитиелю ";". 
    # В данном случае несколько пробелов считаются как один!
    data = line.split(";")
    # Cоздаем пару (key, value):
    # key - product_id
    # value - цена
    # Пишем в STDOUT (standard output) с разделителем \t
    print ('{key}\t{value}'.format(key=int(data[0]), value=float(data[1])))

