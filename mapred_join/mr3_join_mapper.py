#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_join_mapper.py"""

import sys

# Ваш код
# читаем строки из STDIN (standard input)
for line in sys.stdin:
    data = line.strip()
    key, value = data.split('\t', 1)
    print ('{key}\t{value}'.format(key=int(key), value=value))

