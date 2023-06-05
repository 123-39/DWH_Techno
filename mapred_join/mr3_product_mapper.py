#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_product_mapper.py"""

import sys
# Ваш код
# читаем строки из STDIN (standard input)
for line in sys.stdin:
    # Делим строку на слова по пробелам. 
    # В данном случае несколько пробелов считаются как один!
    data = line.strip().split("\t")
    print ('{key}\t{value}'.format(key=int(data[0]), value=data[1]))

